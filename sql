SELECT CONCAT('(', ARRAY_JOIN(COLLECT_LIST(DISTINCT your_column), ', '), ')') AS distinct_values 
FROM your_table;
