"""
This script is the ETL pipeline to create
'people' file and 'aquisition_facts'
"""
import pandas as pd

# Save links to csv files
url1 = 'https://als-hiring.s3.amazonaws.com/fake_data/2020-07-01_17%3A11%3A00/cons.csv'
url2 = 'https://als-hiring.s3.amazonaws.com/fake_data/2020-07-01_17%3A11%3A00/cons_email.csv'
url3 = 'https://als-hiring.s3.amazonaws.com/fake_data/2020-07-01_17%3A11%3A00/cons_email_chapter_subscription.csv'

# Load csv files into Pandas dataframes
cons = pd.read_csv(url1)
cons_email = pd.read_csv(url2)
cons_email_subs = pd.read_csv(url3)

# Set conditions for filtering

# Find primary email
con_1 = cons_email['is_primary'] == 1

# Only concerned with chapter_id=1
con_2 = cons_email_subs['chapter_id'] == 1

# Filter dataframes
cons_filtered = cons[['cons_id', 'source', 'create_dt', 'modified_dt']]
cons_email_filtered = cons_email[con_1][['cons_email_id', 'cons_id', 'email']]
cons_email_subs_filtered = cons_email_subs[con_2][['cons_email_id', 'isunsub']]

# Join data
merge1 = pd.merge(cons_filtered, cons_email_filtered, how='inner', on='cons_id')
merged = pd.merge(merge1, cons_email_subs_filtered, how ='inner', on='cons_email_id')

# Filter join
final = merged[['email', 'source', 'isunsub', 'create_dt', 'modified_dt']]

# Set column names to fit desired schema
final.columns = ['email', 'code', 'is_unsub', 'create_dt', 'updated_dt']

# Extract dates from created datetime
dates = pd.to_datetime(final['create_dt'], infer_datetime_format=True).dt.date

# Calculate number of aquisitions for each date in the dataframe
date_counts = dates.value_counts(sort=False).rename_axis('acquisition_date').reset_index(name='acquisition')

# Save dattaframes as csv files in current working directory
final.to_csv('people.csv', header=True, index=False)
date_counts.to_csv('acquisition_facts.csv', header=True, index=False)