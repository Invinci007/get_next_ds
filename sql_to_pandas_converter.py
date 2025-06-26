import pandas as pd

# Load the datasets
df_s = pd.read_csv('sqlth_partitions_stacked.csv')
df_t = pd.read_csv('ancillary_raw_max_tstamps.csv')

# Step 2: Data type conversion
df_s['end_time'] = pd.to_datetime(df_s['end_time'])
df_t['t_stamp_max'] = pd.to_datetime(df_t['t_stamp_max'])

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
# We use original_dataset_x because after the merge, pandas might suffix columns
# if there are name clashes. Assuming 'original_dataset' from df_s is what's intended.
# If 'original_dataset' in df_s is unique and not clashing, it would be 'original_dataset_x'
# or simply 'original_dataset' if it's the only one.
# Let's check the column names after merge to be sure.
# For this example, 'original_dataset_x' comes from df_s (the left DataFrame).
# The SQL query uses s."original_dataset", which is from the SQLTH_PARTITIONS_STACKED table.
# In our pandas merge, 'original_dataset_x' corresponds to df_s.original_dataset.
merged_df = merged_df[merged_df['original_dataset_x'].str.lower().str.startswith('asc')]

# Output the result
print("Merged DataFrame columns:", merged_df.columns)
print(merged_df)

# To exactly match the SQL's select * behavior, all columns from the merge that satisfy the conditions are kept.
# The 'original_dataset_y' column is from df_t_filtered.
# The SQL query has s."original_dataset" in the where clause, so we filter based on the column from 's'.

# Final selection of columns is implicit in pandas (all columns are kept)
final_df = merged_df

print("\nFinal Resulting DataFrame:")
print(final_df)
