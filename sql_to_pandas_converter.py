import pandas as pd

# Load the datasets
df_s = pd.read_csv('sqlth_partitions_stacked.csv')
df_t = pd.read_csv('ancillary_raw_max_tstamps.csv')

# Step 2: Data type conversion from Unix timestamps
df_s['end_time'] = pd.to_datetime(df_s['end_time'], unit='s')
df_t['t_stamp_max'] = pd.to_datetime(df_t['t_stamp_max'], unit='s')

# Step 3: Filter ANCILLARY_RAW_MAX_TSTAMPS
df_t_filtered = df_t[df_t['site'] == 'ASC']

# Step 4: Merge the DataFrames
# In SQL: on s."pname" = t."original_dataset"
merged_df = pd.merge(df_s, df_t_filtered, left_on='pname', right_on='original_dataset', how='inner')

# Step 5: Apply filtering conditions
# Condition 1: to_date(s."end_time"::varchar)>to_date(t."t_stamp_max"::varchar)
# We compare only the date part.
merged_df = merged_df[merged_df['end_time'].dt.date > merged_df['t_stamp_max'].dt.date]

# Condition 2: s."original_dataset" ilike 'asc%'
# 'original_dataset_x' is the column from df_s after the merge,
# corresponding to s."original_dataset" in the SQL.
merged_df = merged_df[merged_df['original_dataset_x'].str.lower().str.startswith('asc')]

# Output the result
# print("Merged DataFrame columns:", merged_df.columns) # For debugging
# print("Data after conversion and merge (before final filters):") # For debugging
# print(merged_df.to_string()) # For debugging

# Final selection of columns is implicit in pandas (all columns are kept, like SELECT *)
final_df = merged_df

print("\nFinal Resulting DataFrame:")
print(final_df.to_string())
