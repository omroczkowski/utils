-- Step 1: Generate the KPI List Dynamically
DECLARE kpi_list STRING;

SET kpi_list = (
    SELECT STRING_AGG(CONCAT("'", kpi_name, "'"), ', ') 
    FROM (
        SELECT DISTINCT kpi_name 
        FROM table
        WHERE (kpi_name IN (...) OR kpi_name LIKE "..." OR kpi_name LIKE "...")
        AND value_date IN (...)
        AND id1 IN (...)
    )
);

-- Step 2: Construct and Execute the Final PIVOT Query Dynamically
EXECUTE IMMEDIATE '
    SELECT * 
    FROM (
        SELECT id1, id2, kpi_name, kpi_value, value_date 
        FROM table
        WHERE kpi_name IN (' || kpi_list || ')
        AND value_date IN (...)
        AND id1 IN (...)
    ) 
    PIVOT (
        MAX(kpi_value) 
        FOR kpi_name IN (' || kpi_list || ')
    )
';
