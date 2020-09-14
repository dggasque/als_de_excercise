# ALS Hiring

## *Data Engineer Exercise*

### Dependencies

Pandas

### About Script

The script was written using Python 3.7 and Pandas version 0.25.1. There is no need to download the csv files from the source. The script will pull them from the urls provided.

### Running Script

The script can be executed by cloning the repository, navigating to the directory and executing using the python command in your shell.

```
python3 cons_etl.py
```
It will save the two output csv files to the working directory.



### Observations

I made the assumption that the `updated_dt` value was refering to the `modified_dt` column from the `con.csv` file. However, each of the three files had a `modified_dt` column that held different dates.

Some of the `modified_dt` values precede the `created_dt` and will need to be cleaned.

I have kept the original formating for missing values. They are designated by a 'NaN' string.

