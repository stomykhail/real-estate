SELECT 'dim_town' AS table_name, COUNT(*) AS total_rows FROM dim_town
UNION ALL
SELECT 'dim_property_type', COUNT(*) FROM dim_property_type
UNION ALL
SELECT 'dim_date', COUNT(*) FROM dim_date
UNION ALL
SELECT 'dim_address', COUNT(*) FROM dim_address
UNION ALL
SELECT 'fact_household_debt', COUNT(*) FROM fact_household_debt
UNION ALL
SELECT 'fact_unemployment_rate', COUNT(*) FROM fact_unemployment_rate
UNION ALL
SELECT 'fact_affordable_housing', COUNT(*) FROM fact_affordable_housing
UNION ALL
SELECT 'fact_real_estate_transaction', COUNT(*) FROM fact_real_estate_transaction
ORDER BY total_rows DESC;





SELECT 
    COUNT(*) FILTER (WHERE date_recorded_id IS NULL) AS missing_dates,
    COUNT(*) FILTER (WHERE address_id IS NULL) AS missing_addresses,
    COUNT(*) FILTER (WHERE property_type_id IS NULL) AS missing_property_types
FROM fact_real_estate_transaction;




SELECT 
    d.year,
    COUNT(f.transaction_id) AS total_transactions,
    ROUND(AVG(f.sales_amount), 2) AS avg_sale_price,
    ROUND(SUM(f.sales_amount), 2) AS total_sales_volume
FROM fact_real_estate_transaction f
JOIN dim_date d ON f.date_recorded_id = d.date_recorded_id
GROUP BY d.year
ORDER BY d.year DESC;



SELECT 
    t.town_name,
    COUNT(f.transaction_id) AS total_sales
FROM fact_real_estate_transaction f
JOIN dim_address a ON f.address_id = a.address_id
JOIN dim_town t ON a.town_id = t.town_id
GROUP BY t.town_name
ORDER BY total_sales DESC
LIMIT 5;


SELECT 
    transaction_id, 
    assessed_value, 
    sales_amount, 
    sales_ratio 
FROM fact_real_estate_transaction
ORDER BY sales_ratio DESC NULLS LAST
LIMIT 10;


SELECT 
    d.year,
    ROUND(AVG(u.rate), 2) AS avg_unemployment_rate
FROM fact_unemployment_rate u
JOIN dim_date d ON u.date_recorded_id = d.date_recorded_id
GROUP BY d.year
ORDER BY d.year DESC;