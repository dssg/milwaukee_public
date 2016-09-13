import pandas as pd
import numpy as np
import psycopg2
import pandas.io.sql as pdsql
import match
import id_functions
from sqlalchemy import create_engine
import sys
import json

with open(sys.argv[1]) as f:
	creds = json.load(f)

conn = create_engine('postgresql://',connect_args=creds)

juvenile, adult, student = id_functions.load_data(conn)

# For CJ files, split based on defendant_name and clean firstname, lastname
cjFiles = [juvenile, adult]

for i, file in enumerate(cjFiles):
        x = id_functions.split_name(file, 'defendant_name')
        y = id_functions.clean_first_last_name(x, 'firstname', 'lastname')
	cjFiles[i] = y 

# For CJ files, drop duplicates based on exact matches on firstname, lastname, DOB 
# This gives a list of 'unique' individuals trying to generate ids for
juvenile_unique = juvenile.drop_duplicates(subset=['lastname','firstname','defendant_dob'])

matched_df_diff1d, exact_match_df, matched_ratio, exact_match_ratio = id_functions.merge_by_1digitdiff(juvenile_unique, juvenile)

# Subset based on exact matches and remove
#juvenile.loc[exact_match_df.index, 'test_id'] = exact_match_df['unique_index']
juvenile_case_exact_id = juvenile.iloc[exact_match_df.index]
juvenile_case_exact_id['test_id'] = exact_match_df['unique_index']

# Get unique individuals from 1digitdiff
matched_df_diff1d['unique_indiv'] = matched_df_diff1d.groupby(['lastname_cleaned', 'firstname_cleaned', 'year', 'diff_between_dates']).grouper.group_info[0]

juvenile_case_diff1d_id = juvenile.iloc[matched_df_diff1d['index']]
juvenile_case_diff1d_id['test_id'] = matched_df_diff1d['unique_indiv']

# Include lnjaro, fnjaro results (looks like more of the same as above)
match2df, match_ratio2 = match.merge_fn_bdate_lnjaro(juvenile_unique, juvenile)
match3df, match_ratio3 = match.merge_ln_bdate_fnjaro(juvenile_unique, juvenile)

matched_df_diff1d = matched_df_diff1d.rename(columns={'dob_x':'dob'})
match2df = match2df.rename(columns = {'lastname_cleaned_x':'lastname_cleaned'})
match3df = match3df.rename(columns = {'firstname_cleaned_x':'firstname_cleaned'})

juvenile_ids = pd.concat([juvenile_case_exact_id, juvenile_case_diff1d_id, match2df, match3df])

# Write to DB
juvenile_ids.to_sql('juv_case_complete_id', con=engine, schema='cj_schema')





