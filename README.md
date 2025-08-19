# GitHub Issues Pipeline with dlt

> **Note:** This was done on Ubuntu. On other OS, commands may need slight adjustments (e.g., Windows uses `\` in paths).

## What is dlt?

dlt is a minimal, pip-installable Python library for building data pipelines. Similar to how Pandas or NumPy made machine learning accessible to millions, dlt brings the same simplicity and power to data engineering.

## Use Case: GitHub Issues Pipeline

This project demonstrates how to use DLT to build a pipeline that:

- Fetches open issues from a GitHub repository. 
- Transforms and filters them (only real issues, not pull requests).
- Analyzes contributors and calculates simple contribution stats.

## Output Overview

- A table of top contributors ranked by their contribution score.
- Basic statistics such as the number of issues, comments, labels, milestones, and last activity.
- A DuckDB file created locally that stores the processed data.

## Setup & Run

``` bash
# 1. Create environment with Conda
conda create -n dlt_env python=3.10 -y
conda activate dlt_env

# 2. Install dlt
pip install dlt

# 3. Clone the Repository & install all dependencies
git clone https://github.com/enstazao/DLT_Github_Pipeline
cd DLT_Github_Pipeline

pip install -r requirements.txt

# 4. Run the pipeline
python3 github_api_pipeline.py 
# OR
python github_api_pipeline.py

# 5. Inspect the pipeline results
dlt pipeline github_issues_pipeline show

# 6. A DuckDB file will be created in the current working directory

# 7. A Streamlit application will open and display contributions
```

## Demo Screenshots

![App Demo](demo.png "Streamlit Demo")

![Main Concepts of DLT](main-concepts.png "Main Concepts DLT")

## Key Concepts (in simple words)

-   **source**: Defines where you fetch the data from (e.g., GitHub API).
-   **resource**: Handles the actual fetching, pagination, and related logic.
-   **transformer**: A decorator-based function that transforms the data into the desired structure.

## Notes

In this project, DuckDB is used as the destination and GitHub as the source. However, many other sources and destinations are supported by dlt. Refer to the official documentation for more details, they are well-written and very helpful.



# Design Decisions

## Why this API?
GitHub is free, widely used, and familiar to most developers. It provides rich data on issues, comments, labels, and contributors. Also, there’s plenty of material and examples in the **dlt official docs**, which makes learning and extending this pipeline easier.

## How I chosed incremental fields
I used `created_at` and `updated_at` to fetch only new or updated issues since the last run. This makes the pipeline efficient and prevents duplicates.

## What we’d do next with more time
I completed this pipeline in about 4 hours, as I received the email and wanted to submit it quickly. I plan to add logging so that any issues or pipeline progress can be tracked, identify gaps in the code, and further improve it. I also aim to learn more about DLT’s architecture and design principles to explore richer use cases—such as fetching additional GitHub data like pull requests, commits, releases, and other resources. Ultimately, I’d enhance the pipeline and build an interactive app that provides deeper insights and demonstrates the full versatility and efficiency of DLT. I understand that my code may not follow the exact best practices envisioned by the DLT developers, but I plan to continue learning, improve my implementation, and align it more closely with the recommended design patterns and architecture.

# Testing Strategy

## Unit Test
Tests the `filter_valid_issues()` function in isolation to verify it correctly filters GitHub issues, rejecting pull requests, closed issues, and issues with invalid user data while accepting valid open issues.

## Integration Test
Tests the complete dlt pipeline end-to-end, validating the full data flow from API pagination through data transformation to final output, ensuring all business rules and data integrity are maintained.

## Run Tests
```bash
# Run unit tests
  python3 -m pytest unit_tests.py -v

OR 
python -m pytest unit_tests.py -v


# Run integration tests
python3 -m pytest integration_test.py -v

OR 
python -m pytest integration_test.py -v
