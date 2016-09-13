import pandas as pd
import numpy as np
import psycopg2
import pandas.io.sql as pdsql
import match
import jellyfish
from sqlalchemy import create_engine
import sys

with open(sys.argv[1]) as f:
    creds = json.load(f)

engine = create_engine('postgresql://', connect_args=creds)

# Read in data files
juv_case_id = pd.read_sql('SELECT * from cj_schema.juv_case_complete_id', engine)
juv_case_id = juv_case_id[['da_case_#','defendant_name','defendant_dob', 'person_id']]
juv_case_id['lastname'] = juv_case_id['defendant_name'].str.split(", ").str.get(0)
juv_case_id['firstname'] = juv_case_id['defendant_name'].str.split(", ").str.get(1)
juv_case_id = juv_case_id.rename(columns={'defendant_dob':'dob'})

demo = pd.read_sql('SELECT * from edu_schema.demographic', engine)

# Get list of unique students by student key
unique_students = demo[['student_key', 'student_first_name', 'student_last_name', 'student_birthdate']]
unique_students = unique_students.drop_duplicates(subset='student_key', keep='first')
unique_students = unique_students.rename(columns = {'student_last_name':'lastname','student_first_name':'firstname'})

# Clean firstname and lastname
fileList = [juv_case_id, unique_students]
for file in fileList:
	file['lastname_cleaned'] = file['lastname'].astype(str).apply(match.clean_last_name)
	file['firstname_cleaned'] = file['firstname'].astype(str).apply(match.clean_first_name)

# Match on 1 digit difference 
matched_df_diff1d, exact_match_df, matched_ratio, exact_match_ratio = match.merge_by_1digitdiff(juv_case_test, unique_students)

# Subset the important columns and drop duplicates 
# Trying to generate one row per student_key and person_id match 
exact_match_juv_id_student_key = exact_match_df[['da_case_#','defendant_name','dob_x',
	'lastname_cleaned', 'firstname_cleaned','person_id','student_key']]
exact_match_juv_id_student_key = exact_match_juv_id_student_key.drop_duplicates(subset=['person_id','student_key'],keep='first')

# Now match using the fnjaro, lnjaro function
match2df, match_ratio2 = match.merge_fn_bdate_lnjaro(juv_case_id, unique_students)
match3df, match_ratio3 = match.merge_ln_bdate_fnjaro(juv_case_id, unique_students)

matchList = [match2df, match3df]

for file in matchList:  
        file = file.drop_duplicates(subset=['test_id','student_key'],keep='first')

# Clean up some column names
matched_df_diff1d = matched_df_diff1d.rename(columns={'dob_x':'dob'})
match2df = match2df.rename(columns = {'lastname_clean_x':'lastname_clean'})
match3df = match3df.rename(columns = {'firstname_clean_x':'firstname_clean'})

mapping = pd.concat([unique_students, exact_match_juv_id_student_key, match2df, match3df, matched_df_diff1d]).drop_duplicates()

mapping.to_sql('mapping', con=engine, schema='training')
