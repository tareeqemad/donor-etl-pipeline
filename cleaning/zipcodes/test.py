import pandas as pd

contrib = pd.read_csv("contributions_cleaned.csv", dtype=str)
zip_ref = pd.read_csv("../data/zip_code_database.csv", dtype={"zip": str})

# Check for missing state
missing_state = contrib[
    (contrib['contributor_state'].isna() | (contrib['contributor_state'] == ""))
]

# Check for missing city
missing_city = contrib[
    (contrib['contributor_city'].isna() | (contrib['contributor_city'] == ""))
]

# Check for missing state AND city
missing_state_and_city = contrib[
    (contrib['contributor_state'].isna() | (contrib['contributor_state'] == "")) &
    (contrib['contributor_city'].isna() | (contrib['contributor_city'] == ""))
]

# Print results for missing state, city, and both
print(f"Found {len(missing_state)} rows with missing state:")
if not missing_state.empty:
    print(missing_state[['contributor_zip', 'contributor_city', 'contributor_state']])
print("\n")

print(f"Found {len(missing_city)} rows with missing city:")
if not missing_city.empty:
    print(missing_city[['contributor_zip', 'contributor_city', 'contributor_state']])
print("\n")

print(f"Found {len(missing_state_and_city)} rows with both missing state and city:")
if not missing_state_and_city.empty:
    print(missing_state_and_city[['contributor_zip', 'contributor_city', 'contributor_state']])
print("\n")

# Check for ZIP codes that are not 5 digits and not empty
invalid_zips = contrib[
    (contrib['contributor_zip'].str.len() != 5) &
    (contrib['contributor_zip'].notna()) &
    (contrib['contributor_zip'] != "")
]

# Print results for invalid ZIPs
if not invalid_zips.empty:
    print(f"Found {len(invalid_zips)} rows with invalid ZIP codes (not 5 digits and not empty):")
    # print(invalid_zips[['contributor_zip', 'contributor_city', 'contributor_state']])
else:
    print("No invalid ZIP codes found (all are either 5 digits or empty).")
print("\n")

# Preprocess zip_ref the datasbe of zips 
zip_ref['primary_city'] = zip_ref['primary_city'].str.lower().fillna("")
zip_ref['state'] = zip_ref['state'].str.lower().fillna("")
zip_ref['zip'] = zip_ref['zip'].str.zfill(5)

# Create a dictionary for ZIP to city/state mapping
zip_to_info = zip_ref.set_index('zip')[['primary_city', 'state']].to_dict('index')

#print(zip_to_info)
# Function to check for mismatches
def check_zip_mismatch(row):
    zip_code = row['contributor_zip']
    city = str(row['contributor_city']).lower() if pd.notna(row['contributor_city']) else ""
    state = str(row['contributor_state']).lower() if pd.notna(row['contributor_state']) else ""

    # Check if ZIP is 5 digits and exists in zip_ref
    if zip_code and len(zip_code) == 5 and zip_code in zip_to_info:
        zip_info = zip_to_info[zip_code]
        zip_city = zip_info['primary_city']
        zip_state = zip_info['state']
        # Mismatch if neither city nor state matches
        if (city and zip_city != city) and (state and zip_state != state):
            return True
    return False

#  mismatch check
contrib['is_mismatch'] = contrib.apply(check_zip_mismatch, axis=1)

# Filter rows with mismatches
mismatches = contrib[contrib['is_mismatch']][['contributor_zip', 'contributor_city', 'contributor_state']]

# Print results for mismatches
if not mismatches.empty:
    print(f"Found {len(mismatches)} rows with mismatched ZIP codes:")
    # print(mismatches)
else:
    print("No mismatches found (all 5-digit ZIPs match city or state).")
