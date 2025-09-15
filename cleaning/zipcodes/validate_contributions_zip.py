import pandas as pd

# Read the CSV files
contrib = pd.read_csv("../data/contributions.csv", dtype=str)
zip_ref = pd.read_csv("../data/zip_code_database.csv", dtype={"zip": str})

# Create dict for city-to-zip mapping
zip_ref['primary_city'] = zip_ref['primary_city'].str.lower().fillna("")
zip_ref['zip'] = zip_ref['zip'].str.zfill(5)
zip_to_info = zip_ref.set_index('zip')[['primary_city']].to_dict('index')
city_to_zip = zip_ref.groupby('primary_city')['zip'].first().to_dict()

# Normalize ZIP to 5 digits
def normalize_zip(zip_code):
    if pd.isna(zip_code) or not zip_code:
        return ""
    cleaned_zip = ''.join(filter(str.isdigit, zip_code))[:5]
    return cleaned_zip.zfill(5) if cleaned_zip in zip_to_info else ""

contrib['contributor_zip'] = contrib['contributor_zip'].apply(normalize_zip)

# List to store corrected ZIP details
corrected_zips = []

# Function to verify and correct ZIP using city
def verify_and_correct_zip(row):
    current_zip = row['contributor_zip']
    city = str(row['contributor_city']).lower() if pd.notna(row['contributor_city']) else ""
    street = str(row['contributor_street_1']).lower() if pd.notna(row['contributor_street_1']) else ""
    state = str(row['contributor_state']).lower() if pd.notna(row['contributor_state']) else ""
    
    # Verify existing ZIP
    if current_zip and current_zip in zip_to_info:
        zip_city = zip_to_info[current_zip]['primary_city']
        if city and zip_city == city:
            return current_zip  # ZIP is correct
        # Try to correct with city-based lookup
        if city in city_to_zip:
            new_zip = city_to_zip[city]
            if new_zip != current_zip:
                corrected_zips.append({
                    'index': row.name,
                    'original_zip': current_zip,
                    'new_zip': new_zip,
                    'street': street,
                    'city': city,
                    'state': state
                })
            return new_zip
    
    # Fill empty ZIP with city-based lookup
    elif current_zip == "" and city:
        if city in city_to_zip:
            new_zip = city_to_zip[city]
            corrected_zips.append({
                'index': row.name,
                'original_zip': current_zip,
                'new_zip': new_zip,
                'street': street,
                'city': city,
                'state': state
            })
            return new_zip
    
    return current_zip

# Apply ZIP verification and correction
contrib['contributor_zip'] = contrib.apply(verify_and_correct_zip, axis=1)

# Identify rows with missing city AND state
missing_city_state = contrib[
    (contrib['contributor_city'].isna() | (contrib['contributor_city'] == "")) &
    (contrib['contributor_state'].isna() | (contrib['contributor_state'] == ""))
]

# Identify rows with unmatched ZIPs (still empty after processing)
unmatched_zips = contrib[
    (contrib['contributor_zip'].isna() | (contrib['contributor_zip'] == "")) &
    ~((contrib['contributor_city'].isna() | (contrib['contributor_city'] == "")) &
      (contrib['contributor_state'].isna() | (contrib['contributor_state'] == "")))
]

# Drop rows with missing city AND state from cleaned contributions
contrib_cleaned = contrib[
    ~((contrib['contributor_city'].isna() | (contrib['contributor_city'] == "")) &
      (contrib['contributor_state'].isna() | (contrib['contributor_state'] == "")))
]

# Save results
contrib_cleaned.to_csv("contributions_cleaned.csv", index=False)
missing_city_state.to_csv("missing_city_state.csv", index=False)
unmatched_zips.to_csv("unmatched_zips.csv", index=False)

# Save corrected ZIPs
corrected_zips_df = pd.DataFrame(corrected_zips)
if not corrected_zips_df.empty:
    corrected_zips_df.to_csv("corrected_zips.csv", index=False)
    print(f"Found {len(corrected_zips_df)} rows with corrected ZIPs. Details saved to corrected_zips.csv:")
    print(corrected_zips_df[['index', 'original_zip', 'new_zip', 'street', 'city', 'state']])
else:
    print("No ZIPs were corrected.")

# Print summary
print(f"\nTotal original rows: {len(contrib)}")
print(f"Rows with missing city and state: {len(missing_city_state)}")
print(f"Rows with unmatched ZIPs (empty after processing): {len(unmatched_zips)}")
print(f"Rows in cleaned dataset: {len(contrib_cleaned)}")