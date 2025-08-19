import pytest
from github_api_pipeline import filter_valid_issues


def test_filter_valid_issues_comprehensive():
    """
    Comprehensive test for the issue filtering function.
    Tests all the validation rules and edge cases.
    """
    # Test 1: Valid open issue (should pass)
    valid_issue = {
        "pull_request": None,
        "user": {"login": "testuser", "id": 123},
        "state": "open",
        "title": "Test Issue"
    }
    assert filter_valid_issues(valid_issue) == True, "Valid open issue should pass"
    
    # Test 2: Pull request (should fail - not an issue)
    pr_issue = {
        "pull_request": {"url": "https://github.com/pull/1"},
        "user": {"login": "testuser"},
        "state": "open"
    }
    assert filter_valid_issues(pr_issue) == False, "Pull requests should be filtered out"
    
    # Test 3: Closed issue (should fail - only open issues wanted)
    closed_issue = {
        "pull_request": None,
        "user": {"login": "testuser"},
        "state": "closed"
    }
    assert filter_valid_issues(closed_issue) == False, "Closed issues should be filtered out"
    
    # Test 4: Issue without user login (should fail)
    no_login_issue = {
        "pull_request": None,
        "user": {"id": 123},  # missing login
        "state": "open"
    }
    assert filter_valid_issues(no_login_issue) == False, "Issues without user login should fail"
    
    # Test 5: Issue without user object (should fail)
    no_user_issue = {
        "pull_request": None,
        "state": "open"
        # missing user entirely
    }
    assert filter_valid_issues(no_user_issue) == False, "Issues without user should fail"
    
    # Test 6: Edge case - empty dictionary
    assert filter_valid_issues({}) == False, "Empty dictionary should fail"
    
    # Test 7: Edge case - None input
    assert filter_valid_issues(None) == False, "None input should fail"
    
    # Test 8: Edge case - malformed user object
    malformed_user_issue = {
        "pull_request": None,
        "user": "not_a_dict",  # wrong type
        "state": "open"
    }
    assert filter_valid_issues(malformed_user_issue) == False, "Malformed user object should fail"
    
    print("All filter_valid_issues tests passed!")