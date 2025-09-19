import pandas as pd

# Load CSV
csv_path = "data/clean_contrib_INDIV_enriched.csv"
df = pd.read_csv(csv_path)

# Set the character limit of your columns
char_limit = 20

# Identify string columns
string_cols = df.select_dtypes(include="object").columns

# Check for values exceeding limit
for col in string_cols:
    too_long = df[df[col].str.len() > char_limit]
    if not too_long.empty:
        print(f"\nColumn '{col}' has {len(too_long)} values longer than {char_limit}:")
        print(too_long[[col]].head(10))  # show first 10 examples
