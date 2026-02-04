-- Test correct UNNEST syntax for nested struct arrays
WITH data AS (
    SELECT * FROM (VALUES 
        ({'name': 'ok', 'addresses': [{'val': 'no'}, {'val': 'other'}]}),
        ({'name': 'ok3', 'addresses': [{'val': 'single'}]})
    ) AS t(item)
)
-- Method 1: UNNEST in FROM with comma join
SELECT item.name, addr.val 
FROM data, UNNEST(data.item.addresses) AS addr;
