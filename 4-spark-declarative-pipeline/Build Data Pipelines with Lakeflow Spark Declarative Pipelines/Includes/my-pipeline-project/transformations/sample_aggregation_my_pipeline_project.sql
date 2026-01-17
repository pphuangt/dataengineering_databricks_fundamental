-- This file defines a sample transformation.
-- Edit the sample below or add new transformations
-- using "+ Add" in the file browser.

CREATE MATERIALIZED VIEW sample_aggregation_my_pipeline_project AS
SELECT
    user_type,
    COUNT(user_type) AS total_count
FROM sample_users_my_pipeline_project
GROUP BY user_type;
