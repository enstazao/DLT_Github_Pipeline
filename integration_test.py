import pytest
from unittest.mock import patch, MagicMock
from dlt.common import pendulum
from github_api_pipeline import github_api_source, filter_valid_issues, transform_issue_data


def test_complete_pipeline_integration():
    """
    Comprehensive end-to-end test that verifies the complete dlt pipeline
    with proper pagination mocking and data integrity checks.
    """
    
    # Mock data that simulates GitHub API paginated responses
    mock_page_1 = [
        {
            "id": 1,
            "number": 1,
            "title": "Test Issue 1",
            "state": "open",
            "created_at": "2023-01-01T00:00:00Z",
            "updated_at": "2023-01-02T00:00:00Z",
            "comments": 3,
            "labels": [{"name": "bug"}, {"name": "high-priority"}],
            "user": {
                "login": "user1",
                "id": 101,
                "type": "User",
                "html_url": "https://github.com/user1",
                "avatar_url": "https://avatar.com/user1"
            },
            "body": "Issue body 1",
            "assignee": None,
            "milestone": None,
            "pull_request": None
        },
        {
            "id": 2,
            "number": 2,
            "title": "Test Issue 2 - this should be filtered out as PR",
            "state": "open",
            "created_at": "2023-01-03T00:00:00Z",
            "updated_at": "2023-01-04T00:00:00Z",
            "comments": 7,
            "labels": [{"name": "enhancement"}],
            "user": {
                "login": "user2",
                "id": 102,
                "type": "User",
                "html_url": "https://github.com/user2",
                "avatar_url": "https://avatar.com/user2"
            },
            "body": "PR body",
            "assignee": {"login": "user1"},
            "milestone": {"title": "v1.0"},
            "pull_request": {"url": "https://api.github.com/repos/dlt-hub/dlt/pulls/2"}  # This should be filtered
        }
    ]
    
    mock_page_2 = [
        {
            "id": 3,
            "number": 3,
            "title": "Test Issue 3 from second page",
            "state": "open",
            "created_at": "2023-01-05T00:00:00Z",
            "updated_at": "2023-01-06T00:00:00Z",
            "comments": 2,
            "labels": [{"name": "documentation"}],
            "user": {
                "login": "user3",
                "id": 103,
                "type": "User",
                "html_url": "https://github.com/user3",
                "avatar_url": "https://avatar.com/user3"
            },
            "body": "Documentation issue",
            "assignee": None,
            "milestone": None,
            "pull_request": None
        }
    ]
    
    # Mock the paginate function to simulate GitHub API pagination
    with patch('github_api_pipeline.paginate') as mock_paginate:
        
        # Set up the paginate mock to return our test pages
        mock_paginate.return_value = [mock_page_1, mock_page_2]
        
        # Create the source - this returns a DltSource object
        source = github_api_source(access_token="test-token")
        
        # Get the resources from the source
        resources = list(source.resources.values())
        
        # INVARIANT 1: Should return exactly 2 resources (issues and contributors)
        assert len(resources) == 2, "Should return issues and contributors resources"
        
        # Process the issues resource (first resource with transformers)
        # This is a generator, so we need to iterate through it
        issues_resource = resources[0]
        issues = list(issues_resource)
        
        # INVARIANT 2: Should filter out PRs and only include valid issues
        # Page 1: 2 items, 1 PR filtered out → 1 valid issue
        # Page 2: 1 item, 0 PRs filtered out → 1 valid issue
        # Total: 2 valid issues
        assert len(issues) == 2, f"Expected 2 valid issues, got {len(issues)}"
        
        # INVARIANT 3: Verify filtering worked correctly
        issue_numbers = [issue["issue_number"] for issue in issues]
        assert 1 in issue_numbers, "Issue 1 should be included"
        assert 2 not in issue_numbers, "Issue 2 (PR) should be filtered out"
        assert 3 in issue_numbers, "Issue 3 should be included"
        
        # INVARIANT 4: Data transformation integrity
        for issue in issues:
            # Required fields should be present
            assert issue["issue_id"] is not None
            assert issue["issue_number"] is not None
            assert issue["title"] is not None
            assert issue["state"] == "open"
            assert issue["contributor_login"] is not None
            assert issue["contributor_id"] is not None
            
            # Title should be properly truncated
            assert len(issue["title"]) <= 100, "Title should be truncated to 100 chars"
            
            # Transformer outputs should be present
            assert "normalized_title" in issue
            assert "label_count" in issue
            
            # Normalization should work
            assert issue["normalized_title"][0].isupper(), "Title should be capitalized"
            assert issue["normalized_title"].strip() == issue["normalized_title"], "Title should be stripped"
            
            # Label count should match actual labels
            assert issue["label_count"] == len(issue["labels"]), "Label count should match labels array"
        
        # Process the contributors resource (second resource)
        contributors_resource = resources[1]
        contributors = list(contributors_resource)
        
        # INVARIANT 5: Should have contributor stats for all valid contributors
        contributor_logins = [c["contributor_login"] for c in contributors]
        assert "user1" in contributor_logins, "user1 should have contributor stats"
        assert "user3" in contributor_logins, "user3 should have contributor stats"
        assert len(contributors) == 2, f"Expected 2 contributors, got {len(contributors)}"
        
        # INVARIANT 6: Contributor statistics accuracy
        user1_stats = next(c for c in contributors if c["contributor_login"] == "user1")
        user3_stats = next(c for c in contributors if c["contributor_login"] == "user3")
        
        # user1 has 1 issue with 3 comments
        assert user1_stats["total_issues"] == 1
        assert user1_stats["total_comments"] == 3
        assert user1_stats["issues_with_assignee"] == 0
        assert user1_stats["issues_with_milestone"] == 0
        assert user1_stats["labels_count"] == 2  # bug + high-priority
        
        # user3 has 1 issue with 2 comments
        assert user3_stats["total_issues"] == 1
        assert user3_stats["total_comments"] == 2
        assert user3_stats["issues_with_assignee"] == 0
        assert user3_stats["issues_with_milestone"] == 0
        assert user3_stats["labels_count"] == 1  # documentation
        
        # INVARIANT 7: Contribution score calculation
        # user1: 1 issue * 2 + 3 comments * 1 = 5
        assert user1_stats["contribution_score"] == 5.0
        
        # user3: 1 issue * 2 + 2 comments * 1 = 4
        assert user3_stats["contribution_score"] == 4.0
        
        # INVARIANT 8: Timestamps should be properly handled
        for contributor in contributors:
            assert contributor["latest_activity"] is not None
            # Should be a valid ISO timestamp
            pendulum.parse(contributor["latest_activity"])
        
        print("✓ All data integrity invariants maintained")
        print("✓ Pagination handled correctly")
        print("✓ Filtering and transformation working")
        print("✓ Contributor statistics accurate")


def test_direct_resource_execution():
    """
    Test the resources directly without dlt.testing utilities.
    """
    # Mock data
    mock_issue = {
        "id": 999,
        "number": 999,
        "title": "Test Issue for direct execution  ",
        "state": "open",
        "created_at": "2023-01-01T00:00:00Z",
        "updated_at": "2023-01-02T00:00:00Z",
        "comments": 1,
        "labels": [{"name": "test"}],
        "user": {
            "login": "testuser",
            "id": 999,
            "type": "User",
            "html_url": "https://github.com/testuser",
            "avatar_url": "https://avatar.com/testuser"
        },
        "body": "Test body",
        "assignee": None,
        "milestone": None,
        "pull_request": None
    }
    
    with patch('github_api_pipeline.paginate') as mock_paginate:
        mock_paginate.return_value = [[mock_issue]]
        
        # Test the github_api_resource directly
        from github_api_pipeline import github_api_resource, normalize_title, enrich_with_label_counts
        
        # Create the resource with transformers applied
        resource = github_api_resource(access_token="test-token")
        transformed_resource = resource | normalize_title | enrich_with_label_counts
        
        # Execute the resource
        result = list(transformed_resource)
        
        # Verify the result
        assert len(result) == 1
        issue = result[0]
        
        # Check that transformers were applied
        assert "normalized_title" in issue
        assert "label_count" in issue
        assert issue["normalized_title"] == "Test issue for direct execution"
        assert issue["label_count"] == 1
        assert issue["issue_id"] == 999


if __name__ == "__main__":
    test_complete_pipeline_integration()
    test_direct_resource_execution()
    print("All integration tests passed!")