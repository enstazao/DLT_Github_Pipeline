# mypy: disable-error-code="no-untyped-def,arg-type"
from typing import Optional, Dict, Any
import dlt
from dlt.sources.helpers.rest_client import paginate
from dlt.sources.helpers.rest_client.auth import BearerTokenAuth
from dlt.sources.helpers.rest_client.paginators import HeaderLinkPaginator


def filter_valid_issues(item: Dict[str, Any]) -> bool:
    """
    Decide whether an issue from the GitHub API should be included.
    """
    try:
        if item.get("pull_request"):
            return False
        user = item.get("user")
        if not user or not user.get("login"):
            return False
        if item.get("state") != "open":
            return False
        return True
    except (AttributeError, TypeError):
        return False


def transform_issue_data(item: Dict[str, Any]) -> Dict[str, Any]:
    """
    Convert raw GitHub issue data into a structured, analysis-ready format.
    """
    try:
        user = item.get("user", {})
        return {
            "issue_id": item.get("id"),
            "issue_number": item.get("number"),
            "title": item.get("title", "")[:100],
            "state": item.get("state"),
            "created_at": item.get("created_at"),
            "updated_at": item.get("updated_at"),
            "comments_count": item.get("comments", 0),
            "labels": [label.get("name") for label in item.get("labels", []) if label.get("name")],
            "contributor_login": user.get("login"),
            "contributor_id": user.get("id"),
            "contributor_type": user.get("type"),
            "contributor_url": user.get("html_url"),
            "contributor_avatar": user.get("avatar_url"),
            "body_length": len(item.get("body") or ""),
            "has_assignee": bool(item.get("assignee")),
            "milestone": item.get("milestone", {}).get("title") if item.get("milestone") else None,
        }
    except (AttributeError, TypeError) as e:
        print(f"Error transforming item: {e}")
        return None


@dlt.resource(write_disposition="replace")
def github_api_resource(access_token: Optional[str] = dlt.secrets.value):
    """
    A DLT resource that fetches issues from the GitHub API.
    """
    url = "https://api.github.com/repos/dlt-hub/dlt/issues"
    auth = BearerTokenAuth(access_token) if access_token else None
    for page in paginate(
        url,
        auth=auth,
        paginator=HeaderLinkPaginator(),
        params={"state": "open", "per_page": "100"}
    ):
        for item in page:
            if filter_valid_issues(item):
                transformed = transform_issue_data(item)
                if transformed:
                    yield transformed


# ------------------ TRANSFORMERS ------------------

@dlt.transformer
def normalize_title(issue: Dict[str, Any]) -> Dict[str, Any]:
    title = issue.get("title", "")
    issue["normalized_title"] = title.strip().capitalize()
    return issue


@dlt.transformer
def enrich_with_label_counts(issue: Dict[str, Any]) -> Dict[str, Any]:
    labels = issue.get("labels", [])
    issue["label_count"] = len(labels)
    return issue


# ------------------ CONTRIBUTORS ------------------

@dlt.resource(write_disposition="replace")
def top_contributors_resource(access_token: Optional[str] = dlt.secrets.value):
    contributor_stats = {}
    for issue in github_api_resource(access_token=access_token):
        login = issue.get("contributor_login")
        if not login:
            continue
        stats = contributor_stats.setdefault(login, {
            "contributor_login": login,
            "contributor_id": issue.get("contributor_id"),
            "contributor_type": issue.get("contributor_type"),
            "contributor_url": issue.get("contributor_url"),
            "contributor_avatar": issue.get("contributor_avatar"),
            "total_issues": 0,
            "total_comments": 0,
            "total_body_length": 0,
            "issues_with_assignee": 0,
            "issues_with_milestone": 0,
            "labels_used": set(),
            "latest_activity": issue.get("updated_at", ""),
        })
        stats["total_issues"] += 1
        stats["total_comments"] += issue.get("comments_count", 0)
        stats["total_body_length"] += issue.get("body_length", 0)
        if issue.get("has_assignee"):
            stats["issues_with_assignee"] += 1
        if issue.get("milestone"):
            stats["issues_with_milestone"] += 1
        for label in issue.get("labels", []):
            if label:
                stats["labels_used"].add(label)
        updated_at = issue.get("updated_at", "")
        if updated_at and updated_at > stats["latest_activity"]:
            stats["latest_activity"] = updated_at

    for login, stats in contributor_stats.items():
        if stats["total_issues"] > 0:
            stats["avg_body_length"] = (
                stats["total_body_length"] // stats["total_issues"]
                if stats["total_body_length"] > 0 else 0
            )
            stats["labels_count"] = len(stats["labels_used"])
            stats["unique_labels"] = list(stats["labels_used"])
            del stats["total_body_length"], stats["labels_used"]
            stats["contribution_score"] = (
                stats["total_issues"] * 2 +
                stats["total_comments"] * 1 +
                stats["issues_with_assignee"] * 0.5 +
                stats["issues_with_milestone"] * 0.5
            )
            yield stats


# ------------------ SOURCE ------------------

@dlt.source
def github_api_source(access_token: Optional[str] = dlt.secrets.value):
    return [
        (github_api_resource(access_token=access_token)
         | normalize_title
         | enrich_with_label_counts),
        top_contributors_resource(access_token=access_token)
    ]


# ------------------ DISPLAY (FIXED) ------------------

def display_top_contributors(pipeline) -> None:
    """
    Render a ranked summary of top contributors in table form.
    Uses execute_query to avoid the 'list has no attribute fetchall' error.
    """
    try:
        with pipeline.sql_client() as client:
            query = """
            SELECT 
                contributor_login,
                contributor_type,
                total_issues,
                total_comments,
                contribution_score,
                issues_with_assignee,
                issues_with_milestone,
                labels_count,
                latest_activity
            FROM top_contributors_resource
            ORDER BY contribution_score DESC, total_issues DESC
            LIMIT 20
            """
            # execute_query returns a context manager that provides a cursor
            with client.execute_query(query) as cursor:
                rows = cursor.fetchall()
                columns = [desc[0] for desc in cursor.description]

            if rows:
                print("\n" + "="*120)
                print("TOP CONTRIBUTORS ANALYSIS (Sorted by Contribution Score)")
                print("="*120)
                header = f"{'Rank':<5} {'Login':<20} {'Type':<12} {'Issues':<8} {'Comments':<10} {'Score':<8} {'w/Assignee':<12} {'w/Milestone':<12} {'Labels':<8} {'Latest Activity':<20}"
                print(header)
                print("-" * 120)
                
                total_issues_sum = 0
                total_comments_sum = 0
                
                for rank, row in enumerate(rows, 1):
                    row_dict = dict(zip(columns, row))
                    total_issues_sum += row_dict['total_issues']
                    total_comments_sum += row_dict['total_comments']
                    
                    print(f"{rank:<5} "
                          f"{str(row_dict['contributor_login'])[:19]:<20} "
                          f"{str(row_dict['contributor_type']):<12} "
                          f"{row_dict['total_issues']:<8} "
                          f"{row_dict['total_comments']:<10} "
                          f"{row_dict['contribution_score']:<8.1f} "
                          f"{row_dict['issues_with_assignee']:<12} "
                          f"{row_dict['issues_with_milestone']:<12} "
                          f"{row_dict['labels_count']:<8} "
                          f"{str(row_dict['latest_activity'])[:19]:<20}")
                
                print("="*120)
                print(f"Total contributors in top 20: {len(rows)}")
                print(f"Total issues (top 20): {total_issues_sum}")
                print(f"Total comments (top 20): {total_comments_sum}")
                print("="*120)
            else:
                print("No contributor data found.")
                
    except Exception as e:
        print(f"Error displaying contributors: {e}")
        # Fallback to check table existence
        try:
            with pipeline.sql_client() as client:
                tables = client.execute_sql("SHOW TABLES")
                print(f"Available tables: {tables}")
        except Exception as table_error:
            print(f"Could not list tables: {table_error}")


# ------------------ RUN ------------------

def run_source() -> None:
    try:
        pipeline = dlt.pipeline(
            pipeline_name="github_issues_pipeline",
            destination="duckdb",
            dataset_name="github_issues_data"
        )
        print("Starting GitHub API data extraction and transformation...")
        load_info = pipeline.run(github_api_source())
        print("\nPipeline Load Information:")
        print("="*50)
        print(load_info)
        
        display_top_contributors(pipeline)
        
        print("\nPipeline completed successfully!")
    except Exception as e:
        print(f"Error running pipeline: {e}")
        import traceback
        traceback.print_exc()
        raise


if __name__ == "__main__":
    run_source()
