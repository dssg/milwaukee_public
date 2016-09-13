
import pandas as pd
import numpy as np 
import jellyfish 
import datetime as dt


def read_data_clean_column(conn):
    """Queries the database for the students, juvenile and adult offender data and returns
    the content as dataframes.
    :param conn: A psycopg2 connection object
    :return: Pair of dataframes
    :rtype: (DataFrame, DataFrame)
    """
    juvenile = pd.read_sql_query("SELECT * FROM cj_schema.real_criminals_juvenile;", conn)
    adult = pd.read_sql_query("SELECT * FROM cj_schema.real_criminals_adults;", conn)
    student = pd.read_sql_query("SELECT * FROM edu_schema.unique_student_names;", conn)
    student.rename(columns = {'student_first_name': 'firstname', 
                            'student_last_name': 'lastname'}, inplace = True)
    student.rename(columns = {'student_birthdate': 'dob' }, inplace = True)
    juvenile.rename(columns = {'defendant_dob': 'dob' }, inplace = True)
    adult.rename(columns = {'defendant_dob': 'dob' }, inplace = True)
    
    return juvenile, adult, student


def remove_mid_init(name):
    """Removes middle initial from name according to the following algorithm:
        * If the name ends in a '.' or second to last character is ' ', return
          only the first part
    E.g., "Kevin F." ends in a '.' so we return "Kevin"
    :param str name: The first name (potentially including middle initial)
    :return: The cleaned name
    :rtype: str
    """
    #split by space if it ends with . or second character is space
    if name != None:
        name = name.strip()
        if (name.endswith('.') or name[-2:].startswith(' ')) and (' ' in name):
            splitname = name.split(' ')
            fname = splitname[0]
        else:
            fname = name
    else:
        fname = name
    return fname


def remove_tricky_characters(name):
    """Removes tricky chracters, e.g., ', -, III, and so forth
    :param str name: The string to clean
    :return: A cleaned string
    :rtype: str
    """
    if name:
        name = name.replace("'", "")
        name = name.replace('-', '')
        name = name.replace(' III', '')
        name = name.replace(' II', '')
        name = name.replace('II', '')
        name = name.replace('II', '')
        name = name.replace('Jr.', '')
        name = name.replace(' Jr', '')
    return name


def canonicalize_characters(name):
    """Lower case and strip spaces
    :param str name: The name to clean
    :return: The cleaned string
    :rtype: str
    """
    if name:
        name = name.lower()
        name = name.strip()
    return name


def remove_white_space(name):
    """ Remove white space in name
    
    :param str name: The string to clean
    :return: A cleaned string
    :rtype: str
    """
    if name != None:
        name = name.replace(" ", "")
        name = name.lower()
    else:
        name = None
    return name


def split_last_name(name):
    """Split last name if last name contains -
    E.g. "Morales-Hernandez" has a -, so returns "Morales"
    :param str firstpart_lname
    :return: the first part of the last name
    :rtype: str
    """    
    if not name:
        return name
    return name.split('-', 1)[0]


def clean_first_name(name):
    """Meta function to clean up first names
    
    :param str name
    :return: A cleaned string
    :rtype: str
    """
    name = remove_mid_init(name)
    name = remove_tricky_characters(name)
    name = canonicalize_characters(name)
    return name


def clean_last_name(name):
    """Meta function to clean up names
    
    :param str name
    :return: A cleaned string
    :rtype: str
    """
    name = split_last_name(name)
    name = remove_white_space(name)
    return name


def num_diff_digit(date_a, date_b):
    """ Calculate the number of different digit between 2 strings (dates)
    E.g. given "1-1-1911", "1-2-1911", the function returns 1
    
    :param str date_a
    :param str date_b
    :return: an interger that represents the number of different digits between 
    2 date strings
    :rtype: integer
    """
    return sum (date_a[i] != date_b[i]  for i in range(len(date_a)))


def merge_by_1digitdiff(df1, df2, df1_dob = 'dob', df2_dob = 'dob'):
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
    df1['df1_dob'] = df1[df1_dob].astype('str')
    df2['df2_dob'] = df2[df2_dob].astype('str')
    df1['df1_index'] = df1.index
    df2['df2_index'] = df2.index
 
    matched_df_yr = pd.merge(df1, df2, on = ['firstname_cleaned','lastname_cleaned','year'])
    matched_df_yr['diff_between_dates'] = matched_df_yr.apply(lambda row: num_diff_digit(
            row['df1_dob'], row['df2_dob']), axis=1)
    matched_ratio = (matched_df_yr['diff_between_dates']==1).sum()/len(df2)
    matched_df_diff1d = matched_df_yr[matched_df_yr['diff_between_dates']==1]
    exact_match_ratio = (matched_df_yr['diff_between_dates']==0).sum()/len(df2)
    exact_match_df = matched_df_yr[matched_df_yr['diff_between_dates']==0]
    
    return matched_df_diff1d, exact_match_df, matched_ratio, exact_match_ratio


def merge_fn_bdate_lnjaro(df1, df2, threshold = 0.8):
    """ Merge the student and defendant records by first exact matching 
    "lastname_clean" and "STUDENT_BIRTHDATE" and then calculate the jaro distance of the 
    first names. Records are considered matched if the jaro score is above the threshold
    :param DataFrame df1: The left dataframe
    :param DataFrame df2: The right dataframe 
    :return: matched dataframe, matching % between student and defendant records.
    :rtype: (Dataframe, float)
    """
    jaro = pd.merge(df1, df2, left_on =['firstname_clean','dob'], 
                  right_on =['firstname_clean','dob'])
    jaro['jaro_score'] = jaro.apply(lambda row: jellyfish.jaro_distance(
            row['lastname_clean_x'],row['lastname_clean_y']), axis=1)
    matched_df = jaro[(jaro['jaro_score'] >= threshold) & (jaro['jaro_score'] <1)] 
    #jaro_score = 1 indicates exact match of first name
    matched_ratio = len(matched_df)/len(df2)

    return matched_df, matched_ratio


def merge_ln_bdate_fnjaro(df1, df2, threshold = 0.8):
    """ Merge the student and defendant records by first exact matching 
    "lastname_clean" and "STUDENT_BIRTHDATE"and then calculate the jaro distance of the 
    first names. Records considered matched if the jaro score is above the threshold
    :param DataFrame df1: The left dataframe
    :param DataFrame df2: The right dataframe
    :return: matched dataframe, matching % between student and defendant records.
    :rtype: (Dataframe, float) 
    """
    jaro = pd.merge(df1, df2, left_on =['lastname_clean','dob'], 
                  right_on =['lastname_clean','dob'])
    jaro['jaro_score_fn'] = jaro.apply(lambda row: jellyfish.jaro_distance(
            row['firstname_clean_x'],row['firstname_clean_y']), axis=1)
    matched_df = jaro[(jaro['jaro_score_fn'] >= threshold) & (jaro['jaro_score_fn'] <1)] 
    #jaro_score = 1 indicates exact match of first name

    matched_ratio = len(matched_df)/len(df2)

    return matched_df, matched_ratio
