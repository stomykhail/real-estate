import pandas as pd
import numpy as np
import re

class ConnecticutRealEstateETL:
    """
    Production ETL Pipeline for the Connecticut Real Estate Galaxy Schema.
    Handles data ingestion, dimension modeling (Surrogate Keys), and Fact table generation.
    """
    def __init__(self):
        self.raw_paths = {
            "real_estate": "CSVs/Real_Estate_Sales_2001-2023.csv",
            "unemployment": "CSVs/UNEMPLOYCT.csv",
            "debt": "CSVs/household-debt.csv",
            "housing": "CSVs/Affordable_Housing.csv"
        }
        self.output_dir = "Clean_Output/"
        
        # In-memory dataframes for the Galaxy Schema
        self.raw = {}
        self.dims = {}
        self.facts = {}

        import os
        os.makedirs(self.output_dir, exist_ok=True)

    def extract_raw_data(self):
        """Step 1: Extract all raw CSVs into memory."""
        print("Extracting raw data...")
        self.raw["real_estate"] = pd.read_csv(self.raw_paths["real_estate"], low_memory=False)
        self.raw["unemployment"] = pd.read_csv(self.raw_paths["unemployment"])
        self.raw["debt"] = pd.read_csv(self.raw_paths["debt"])
        self.raw["housing"] = pd.read_csv(self.raw_paths["housing"])
        return self

    def transform_dimensions(self):
        """Step 2: Build Conformed Dimensions with Surrogate Keys."""
        print("Building Dimension Tables...")
        re_df = self.raw["real_estate"]

        # 1. TOWN DIMENSION
        towns = re_df[["Town"]].drop_duplicates().dropna().reset_index(drop=True)
        towns.index += 1  # Start ID at 1
        towns = towns.reset_index().rename(columns={"index": "Town ID"})
        self.dims["town"] = towns

        # 2. PROPERTY TYPE DIMENSION
        prop_types = re_df[["Property Type"]].drop_duplicates().dropna().reset_index(drop=True)
        prop_types.index += 1
        prop_types = prop_types.reset_index().rename(columns={"index": "Property Type ID"})
        self.dims["property_type"] = prop_types

        # 3. DATE DIMENSION (Extracting from all fact sources)
        # Parse Real Estate Dates
        re_dates = pd.to_datetime(re_df["Date Recorded"], errors="coerce").dropna()
        # Parse Unemployment Dates
        unemp_dates = pd.to_datetime(self.raw["unemployment"]["observation_date"])
        # Parse Debt Dates (Map Qtr to Month)
        debt = self.raw["debt"][self.raw["debt"]["state_fips"].astype(str).str.zfill(2) == "09"].copy()
        debt_dates = pd.to_datetime({"year": debt["year"], "month": debt["qtr"].map({1:1, 2:4, 3:7, 4:10}), "day": 1})
        # Combine all unique dates
        all_dates = pd.concat([re_dates, unemp_dates, debt_dates]).dt.normalize().drop_duplicates().reset_index(drop=True)
        
        # Build Date Dim
        date_dim = pd.DataFrame({
            "full_date": all_dates,
            "Month": all_dates.dt.month,
            "Day": all_dates.dt.day,
            "Quarter": all_dates.dt.quarter,
            "Year": all_dates.dt.year
        })
        date_dim.index += 1
        date_dim = date_dim.reset_index().rename(columns={"index": "Date ID"})
        date_dim["full_date"] = date_dim["full_date"].dt.strftime("%m/%d/%y") # Format to match requirement
        self.dims["date"] = date_dim

        # 4. ADDRESS DIMENSION (With Regex Geocoding)
        addresses = re_df[["Address", "Town", "Location"]].drop_duplicates().dropna(subset=["Address"]).copy()
        
        # Merge with Town ID
        addresses = addresses.merge(towns, on="Town", how="left")
        
        # Extract Longitude/Latitude via Regex
        coords = addresses["Location"].str.extract(r"POINT \(([-\d\.]+) ([-\d\.]+)\)")
        addresses["Longitude"] = pd.to_numeric(coords[0], errors="coerce")
        addresses["Latitude"] = pd.to_numeric(coords[1], errors="coerce")
        
        addresses = addresses[["Address", "Town ID", "Longitude", "Latitude"]].drop_duplicates().reset_index(drop=True)
        addresses.index += 1
        addresses = addresses.reset_index().rename(columns={"index": "Address ID"})
        self.dims["address"] = addresses

        return self

    def transform_facts(self):
        """Step 3: Build Fact Tables by mapping Surrogate Keys from Dimensions."""
        print("Building Fact Tables...")
        
        # Helper dictionary for fast Date lookups
        date_map = dict(zip(self.dims["date"]["full_date"], self.dims["date"]["Date ID"]))

        # --- 1. REAL ESTATE FACT ---
        re_df = self.raw["real_estate"].copy()
        re_df["formatted_date"] = pd.to_datetime(re_df["Date Recorded"], errors="coerce").dt.strftime("%m/%d/%y")
        
        # Merge Dimensions to get IDs
        re_fact = re_df.merge(self.dims["property_type"], on="Property Type", how="left")
        re_fact = re_fact.merge(self.dims["address"], on="Address", how="left")
        re_fact["date_recorded_id"] = re_fact["formatted_date"].map(date_map)
        
        # Select Final Columns
        re_fact = re_fact[["Serial Number", "Assessed Value", "Sale Amount", "Sales Ratio", 
                           "Address ID", "Property Type ID", "date_recorded_id"]].copy()
        re_fact.columns = ["serial_number", "assessed_value", "sales_amount", "sales_ratio", 
                           "address_id", "property_type_id", "date_recorded_id"]
        re_fact.insert(0, "transaction_id", range(len(re_fact))) # Generate Transaction ID
        self.facts["real_estate"] = re_fact

        # --- 2. UNEMPLOYMENT FACT ---
        unemp = self.raw["unemployment"].copy()
        unemp["formatted_date"] = pd.to_datetime(unemp["observation_date"]).dt.strftime("%m/%d/%y")
        unemp["date_recorded_id"] = unemp["formatted_date"].map(date_map)
        
        unemp_fact = unemp[["UNEMPLOYCT", "date_recorded_id"]].copy()
        unemp_fact.columns = ["rate", "date_recorded_id"]
        self.facts["unemployment"] = unemp_fact

        # --- 3. HOUSEHOLD DEBT FACT ---
        debt = self.raw["debt"][self.raw["debt"]["state_fips"].astype(str).str.zfill(2) == "09"].copy()
        debt["formatted_date"] = pd.to_datetime({"year": debt["year"], "month": debt["qtr"].map({1:1, 2:4, 3:7, 4:10}), "day": 1}).dt.strftime("%m/%d/%y")
        debt["date_recorded_id"] = debt["formatted_date"].map(date_map)
        
        debt_fact = debt[["low", "high", "date_recorded_id"]].copy()
        debt_fact.columns = ["lower_bound", "upper_bound", "date_recorded_id"]
        self.facts["household_debt"] = debt_fact

        # --- 4. AFFORDABLE HOUSING FACT ---
        housing = self.raw["housing"].copy()
        housing = housing.merge(self.dims["town"], on="Town", how="left")
        
        # Group to find macro trends
        housing_grouped = housing.groupby(["Year", "Town ID"], as_index=False)[["Census Units", "Total Assisted Units"]].sum()
        housing_grouped["rate"] = (housing_grouped["Total Assisted Units"] / housing_grouped["Census Units"] * 100).round(2)
        
        # Map year to the first date of that year in the date dimension
        year_to_date_id = self.dims["date"].drop_duplicates(subset=["Year"], keep="first").set_index("Year")["Date ID"].to_dict()
        housing_grouped["date_recorded_id"] = housing_grouped["Year"].map(year_to_date_id)
        
        housing_fact = housing_grouped[["Census Units", "Total Assisted Units", "rate", "Town ID", "date_recorded_id"]].copy()
        housing_fact.columns = ["census_units", "total_assisted_units", "rate", "town_id", "date_recorded_id"]
        self.facts["affordable_housing"] = housing_fact

        return self

    def load_clean_csvs(self):
        """Step 4: Export everything to the target format."""
        print("Exporting to CSV...")
        
        # Dimensions
        for name, df in self.dims.items():
            df.to_csv(f"{self.output_dir}{name}.csv", index=False)
            
        # Facts
        for name, df in self.facts.items():
            df.to_csv(f"{self.output_dir}{name}.csv", index=False)
            
        print("✅ Pipeline Complete. All Galaxy Schema files generated successfully.")

# === EXECUTION ===
if __name__ == "__main__":
    pipeline = ConnecticutRealEstateETL()
    pipeline.extract_raw_data() \
            .transform_dimensions() \
            .transform_facts() \
            .load_clean_csvs()