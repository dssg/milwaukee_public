import pandas as pd
import numpy as np 


def gen_id_linked(df):
    """generate person_id, a unique id, for the matched individuals 
    
    :param df: A dataframe object for the linked individuals
    :return: A dataframe wiith the Person ID of the linked individuals
    :rtype df: A dataframe 
    """
    
    N = len(df)
    tmp = pd.DataFrame({'person_id': range(1, N+1 ,1 )})
    df = df.reset_index()
    df = pd.concat([df, tmp], axis=1, ignore_index = True)
    df.columns = ['index_0', 'STUDENT_KEY', 'firstname_clean','lastname_clean','dob' ,'tmp']
    df['person_id'] = 'L'+ df['tmp'].map(str)
    df = df.drop(['tmp'], 1)
    
    return df
    

def gen_id_unlinked_student(df, matched_df):
    """generate person_id, a unique id, for student who did not appear in the CJ system
    
    :param df: A dataframe object for the demographic file 
    :param matched_df: A dataframe for the matched individuals   
    :return: A dataframe wiith the Person ID of the unlinked students
    :rtype df: A dataframe 
    """
    demo_unique_stu_key = pd.DataFrame(df['STUDENT_KEY'].unique())
    demo_unique_stu_key.columns = ['STUDENT_KEY']
    unmatched_student_in_demo =demo_unique_stu_key[(~demo_unique_stu_key['STUDENT_KEY'].
                                                    isin(matched_df['STUDENT_KEY']))]
    N = len(unmatched_student_in_demo)
    tmp = pd.DataFrame({'tmp': range(1, N+1 ,1 )})
    unmatched_student_in_demo = unmatched_student_in_demo.reset_index()
    unmatched_student_in_demo = pd.concat([unmatched_student_in_demo, tmp], axis=1, ignore_index = True)
    unmatched_student_in_demo.columns = ['index_0', 'STUDENT_KEY', 'tmp']
    unmatched_student_in_demo['person_id'] = 'D'+ unmatched_student_in_demo['tmp'].map(str)
        
    return unmatched_student_in_demo


def gen_id_unlinked_cj(cj_df, matched_df):
    """generate person_id, a unique id, for individuals in the criminal justice
    (juv / adult) file whom are unmatched to the edu demographic file

    :param juv_df: dataframe with list of juveniles
    :param matched_df: dataframe with list of linked individuals
    :return: A dataframe with the Person ID of unlinked juvneiles 
    :rtype df: A dataframe 
    """
    tmp = pd.merge(cj_df, matched_df, how = 'left', 
                   on = ['firstname_clean','lastname_clean','dob'] ) 
    unmatched = tmp[tmp['person_id'].isnull()]
    unmatched = unmatched[['firstname_clean','lastname_clean','dob']]
    N = len(unmatched)
    tmp = pd.DataFrame({'tmp': range(1, N+1 ,1 )})
    unmatched = unmatched.reset_index()
    
    unmatched = pd.concat([unmatched, tmp], axis=1, ignore_index = True)
    unmatched.columns = ['index_0', 'firstname_clean', 'lastname_clean','dob' ,'tmp']
    unmatched['person_id'] = 'C'+ unmatched['tmp'].map(str)

    unmatched = unmatched.drop(['tmp'], 1)
    
    return unmatched


def gen_table_id(df1 , df2 , df3 ):
    """merge the dataframes with person id of these groups: linked, unlinked student, 
    unlinked juvenile, and generate a column that indicates their linked status.

    :return: A dataframe with the Person ID for all individuals in demographic and juvenile file
    :rtype df: A dataframe 
    """
  
    tmp = df1.append(df2)
    person_demo_cj = tmp.append(df3)
    person_demo_cj['Linked'] = 0
    person_demo_cj['In_Demo_only'] = 0
    person_demo_cj['In_CJ_only'] = 0
    person_demo_cj.ix[person_demo_cj['person_id'].str[:1] == 'L','Linked'] = 1
    person_demo_cj.ix[person_demo_cj['person_id'].str[:1] == 'D','In_Demo_only'] = 1
    person_demo_cj.ix[person_demo_cj['person_id'].str[:1] == 'C','In_CJ_only'] = 1
    person_demo_cj = person_demo_cj.drop(['index_0'], axis =1)
    
    return person_demo_cj
                 
                 
