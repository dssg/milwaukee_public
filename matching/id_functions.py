import match
import pandas as pd

def load_data(conn):
    juvenile = pd.read_sql_query('SELECT "da_case_#", defendant_name, defendant_dob from cj_schema.juvenile_case', conn)
    adult = pd.read_sql_query('SELECT "da_case_#", defendant_name, defendant_dob from cj_schema.adult_case',conn)
    student = pd.read_sql_query('SELECT student_key, student_first_name, student_last_name, student_birthdate from edu_schema.demographic', conn)
    return juvenile, adult, student
    
def split_name(df, name_col):
	df['firstname'] = df[name_col].astype(str).str.split(",").str.get(0)
	df['lastname'] = df[name_col].astype(str).str.split(",").str.get(1)
	return df

def clean_first_last_name(df, firstname_col, lastname_col):
    df['firstname_cleaned'] = df[firstname_col].astype(str).apply(match.clean_last_name)
    df['lastname_cleaned'] = df[lastname_col].astype(str).apply(match.clean_first_name)
    return df

def generate_groupby_ids(df):
	df = df.drop_duplicates()
	df['temp_ids'] = df.groupby(['firstname_cleaned', 'lastname_cleaned', 'defendant_dob']).grouper.group_info(0)
	return df

def merge_ids_df(df_to_match, df_with_ids):
	matched_df = pd.merge(df_to_match, df_with_ids, 
			on = ['dob', 'lastname_cleaned', 'firstname_cleaned'])
	return matched_df

def merge_by_1digitdiff(df1, df2, df1_dob = 'defendant_dob', df2_dob = 'defendant_dob' ):
    """ Merge the student and defendant records by exact matching these 2 fields: 
    "firstname_clean", "lastname_clean", "year" and 
    then any date entries that only differ by less than 2 digits. 
   
    :param DataFrame df1: The left dataframe
    :param DataFrame df2: The right dataframe
    :return: the matched dataframe, matching % between student and defendant records.
    :rtype: (Dataframe, float) 
    """
    df1['year'] = df1[df1_dob].astype('datetime64[ns]').dt.year
    df2['year'] = df2[df2_dob].astype('datetime64[ns]').dt.year
    df1['full_dob'] = df1[df1_dob].astype('str')
    df2['unique_dob'] = df2[df2_dob].astype('str')
    df1['index'] = df1.index
    df2['unique_index'] = df2.index

    matched_df_yr = pd.merge(df1, df2, on = ['firstname_clean','lastname_clean','year'], right_index=True)
    matched_df_yr['diff_between_dates'] = matched_df_yr.apply(lambda row: match.num_diff_digit(
            row['full_dob'], row['unique_dob']), axis=1)
    matched_ratio = (matched_df_yr['diff_between_dates']==1).sum()/len(df2)
    matched_df_diff1d = matched_df_yr[matched_df_yr['diff_between_dates']==1]
    exact_match_ratio = (matched_df_yr['diff_between_dates']==0).sum()/len(df2)
    exact_match_df = matched_df_yr[matched_df_yr['diff_between_dates']==0]
    
    return matched_df_diff1d, exact_match_df, matched_ratio, exact_match_ratio

def make_exact_ids(exact_match_df, df_to_subset):
	exact_match_id = df_to_subset.iloc[exact_match_df['unique_index']]
	exact_match_id['id'] = exact_match_df['unique_index']
	return exact_match_id

def generate_universal_id(id_df):
	id_df.person_id.fillna(id_df.student_key, inplace=True)
	return id_df

