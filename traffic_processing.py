import pandas as pd
import os

base_path = 'C:/Users/Carson/Desktop/toronto traffic project/'
file_path = os.path.join(base_path, 'data', 'raw', 'Traffic Collisions - 4326.csv')

df = pd.read_csv(file_path)

# print(df.info())
# print(df.head())

# print(df.describe()) # For numerical columns
# print(df.describe(include='object')) # For categorical/text columns

# print(df.isnull().sum())


# ================================= Clean Columns with 'YES'/'NO'/'N/R' into 1s and 0s =================================

# These columns are actually Boolean indicators, not numerical counts
yes_no_count_columns = [
    'INJURY_COLLISIONS', 'PD_COLLISIONS', 'FTR_COLLISIONS', 
    'AUTOMOBILE', 'MOTORCYCLE', 'PASSENGER', 'BICYCLE', 'PEDESTRIAN'
]

# Standardize values to uppercase and replace with binary
for col in yes_no_count_columns:
    df[col] = df[col].astype(str).str.upper().map({'YES': 1, 'NO': 0, 'N/R': 0}).fillna(0).astype(int)

# Fix FATALITIES column: keep valid numbers, convert blanks/missing to 0
df['FATALITIES'] = pd.to_numeric(df['FATALITIES'], errors='coerce').fillna(0).astype(int)

columns_to_check = yes_no_count_columns + ['FATALITIES']

# --- Check the result ---
# for col in columns_to_check:
#     print(f"\n--- {col} ---")
#     print(df[col].value_counts(dropna=False).sort_index())






# ================================= cleaning nsa or other stuff in object columns ================================= 



# Define a list of common missing value representations you want to treat as NaN
missing_value_markers = ['nsa', 'N/R', 'N/A', '-', '', 'NSA'] # Add any others you find

# Apply this replacement across your DataFrame for relevant columns
# You can do this on specific columns or the entire DataFrame if confident
for col in df.columns:
    if df[col].dtype == 'object': # Only apply to object (string) columns
        df[col] = df[col].replace(missing_value_markers, pd.NA) # pd.NA is better for mixed types
        # df[col] = df[col].fillna('Unknown')

# --- Handle geographical columns (DIVISION, HOOD_158, NEIGHBOURHOOD_158) ---
# Fill pd.NA values with the string 'Unknown' to preserve rows and allow for analysis
geo_columns_to_fill = ['DIVISION', 'HOOD_158', 'NEIGHBOURHOOD_158']
for col in geo_columns_to_fill:
    df[col] = df[col].fillna('Unknown') # Fill with string 'Unknown'


# --- Show the results of the replacement ---
# print("--- Missing values after converting 'nsa', 'N/R', etc. to NaN/NA ---")
# print(df.isnull().sum())




# ================================= Date/Time Transformations ================================= 

# 1. Convert OCC_DATE to datetime objects, specifying unit='ms' for milliseconds
df['OCC_DATE'] = pd.to_datetime(df['OCC_DATE'], unit='ms', errors='coerce')

# 2. Combine OCC_DATE and OCC_HOUR into a single OCC_DATETIME column
# Ensure OCC_HOUR is treated as an integer for the 'hour' parameter
df['OCC_DATETIME'] = df.apply(lambda row: row['OCC_DATE'].replace(hour=int(row['OCC_HOUR'])), axis=1)

# 3. Extract the 'Day of the Month'
df['OCC_DAY_OF_MONTH'] = df['OCC_DATETIME'].dt.day

# 4. Extract other useful temporal features
df['Is_Weekend'] = df['OCC_DATETIME'].dt.dayofweek >= 5 


# Convert OCC_MONTH from month name to month number (January = 1, ..., December = 12)
month_mapping = {
    'January': 1, 'February': 2, 'March': 3, 'April': 4,
    'May': 5, 'June': 6, 'July': 7, 'August': 8,
    'September': 9, 'October': 10, 'November': 11, 'December': 12
}
df['OCC_MONTH'] = df['OCC_MONTH'].map(month_mapping)

# Convert OCC_DOW from day name to number (Sunday = 0, ..., Saturday = 6)
dow_mapping = {
    'Sunday': 0, 'Monday': 1, 'Tuesday': 2, 'Wednesday': 3,
    'Thursday': 4, 'Friday': 5, 'Saturday': 6
}
df['OCC_DOW'] = df['OCC_DOW'].map(dow_mapping)

# # Show the first few rows of the relevant columns to verify the transformations
# print(df[['OCC_DATE', 'OCC_HOUR', 'OCC_DATETIME', 'OCC_DAY_OF_MONTH', 'Is_Weekend', 'OCC_MONTH', 'OCC_DOW']].head(10))

# # Also, display counts of unique values to verify mappings
# print("\nUnique OCC_MONTH values and counts:")
# print(df['OCC_MONTH'].value_counts(dropna=False).sort_index())

# print("\nUnique OCC_DOW values and counts:")
# print(df['OCC_DOW'].value_counts(dropna=False).sort_index())




# ================================= Flag rows with (0.0, 0.0) coordinates ================================= 


# Add a flag for valid geolocation
df['Has_Valid_Location'] = ~((df['LONG_WGS84'] == 0.0) & (df['LAT_WGS84'] == 0.0))

# Count how many rows have invalid coordinates
invalid_coords_count = (~df['Has_Valid_Location']).sum()
# print(f"Rows with (0.0, 0.0) coords: {invalid_coords_count}")

# Drop the redundant geometry column
df.drop(columns=['geometry'], inplace=True)

# Display 5 sample values from every column
sample_summary = df.apply(lambda col: col.sample(5, random_state=42).tolist()).T
sample_summary.columns = [f'Sample_{i+1}' for i in range(5)]

# Show the samples in a transposed table for readability
# print(sample_summary)




# # ================================= Data Checking ================================= 

# Numerical summary for all numeric columns
print("=== Numerical Summary ===")
print(df.describe(include=[int, float]))

# Summary for all object (string) and categorical columns
print("\n=== Categorical / Object Summary ===")
print(df.describe(include=[object, 'category']))

# Summary for boolean columns (if any)
bool_cols = df.select_dtypes(include=['bool']).columns
if len(bool_cols) > 0:
    print("\n=== Boolean Summary ===")
    print(df[bool_cols].describe())
else:
    print("\nNo boolean columns found.")

# Optionally, show counts of missing values for all columns:
print("\n=== Missing Values per Column ===")
print(df.isna().sum())



# ================================= Exporting ================================= 

# Path to save cleaned data
cleaned_path = os.path.join(base_path, 'data', 'cleaned_collision_data.csv')

# Export to CSV
df.to_csv(cleaned_path, index=False)