SELECT CONCAT('ALTER table cj_schema.test_table',
              ' rename column "', column_name,
              '" to "', REPLACE(LOWER(column_name), ' ', '_'),
              '";')
FROM information_schema.columns
WHERE table_schema = 'cj_schema';
