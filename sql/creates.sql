CREATE TABLE dim_town (
    town_id INT PRIMARY KEY,
    town_name VARCHAR(100)
);

-- Date Dimension
CREATE TABLE dim_date (
    date_recorded_id INT PRIMARY KEY,
    full_date DATE,
    day INT,
    month INT,
    quarter INT,
    year INT
);

-- Property Type Dimension
CREATE TABLE dim_property_type (
    property_type_id INT PRIMARY KEY,
    type VARCHAR(100)
);

-- Address Dimension (Now includes Latitude & Longitude!)
CREATE TABLE dim_address (
    address_id INT PRIMARY KEY,
    address VARCHAR(255),
    town_id INT REFERENCES dim_town(town_id),
    latitude DECIMAL(10, 6),
    longitude DECIMAL(10, 6)
);



-- Real Estate Transaction Fact
-- Note: transaction_id uses INT because your CSV already has an exact 'ID' column
CREATE TABLE fact_real_estate_transaction (
    transaction_id INT PRIMARY KEY,
    serial_number INT,
    date_recorded_id INT REFERENCES dim_date(date_recorded_id),
    address_id INT REFERENCES dim_address(address_id),
    property_type_id INT REFERENCES dim_property_type(property_type_id),
    assessed_value DECIMAL(15, 2),
    sales_amount DECIMAL(15, 2),
    sales_ratio DECIMAL(15, 4)
);

-- Household Debt Fact
-- Note: debt_id uses SERIAL so Postgres generates it automatically
CREATE TABLE fact_household_debt (
    debt_id SERIAL PRIMARY KEY,
    date_recorded_id INT REFERENCES dim_date(date_recorded_id),
    lower_bound DECIMAL(10, 4),
    upper_bound DECIMAL(10, 4)
);

-- Unemployment Rate Fact
CREATE TABLE fact_unemployment_rate (
    rate_id SERIAL PRIMARY KEY,
    date_recorded_id INT REFERENCES dim_date(date_recorded_id),
    rate DECIMAL(10, 4)
);

-- Affordable Housing Fact
CREATE TABLE fact_affordable_housing (
    affordable_housing_id SERIAL PRIMARY KEY,
    date_recorded_id INT REFERENCES dim_date(date_recorded_id),
    town_id INT REFERENCES dim_town(town_id),
    census_units INT,
    total_assisted_units INT,
    rate DECIMAL(10, 4)
);



