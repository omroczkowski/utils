SELECT CONCAT('(', STRING_AGG(DISTINCT your_column, ', '), ')') AS distinct_values 
FROM your_table;
