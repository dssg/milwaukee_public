import pandas as pd
import numpy as np
import psycopg2
import pandas.io.sql as pdsql
from sqlalchemy import create_engine
    
def gen_label(conn):
    """
    generates all the necessary labels in the database as training.labels. The labels are 
    binary object in the table that indicates whether a student had an interaction with 
    the criminal justice.
    """
    # Load in data
    juvenile_case = pd.read_sql_query('''SELECT "da_case_#", person_id, incident_date 
                                      from cj_schema.juv_case_person_id''', conn)

    # Get useful rows
    juv_case_unique = juvenile_case.drop_duplicates()

    # Extract year information -- add as a new column
    juv_case_unique['year'] = juv_case_unique['incident_date'].astype('datetime64[ns]').dt.year

    # Get labelled year information -- dummify incident year information
    # Get a count of 1 for each incident year 
    # Delete unnecessary columns  
    label_years = pd.concat([juv_case_unique, pd.get_dummies(juv_case_unique['year'])], axis=1)
    del label_years['da_case_#']
    del label_years['incident_date']
    del label_years['year']

    # Change the column names so that they work in postgres
    label_years.columns = 'year_' + label_years.columns.astype(str)
    label_years = label_years.rename(columns={'year_person_id':'person_id'})

    # Remove the duplicate rows - sum the columns per person_id
    # Use as_index = False to leave person_id column in 
    label_years_final = label_years.groupby('person_id', as_index=False).sum()

    # Read in overall IDs
    ids = pd.read_sql("SELECT person_id, student_key from training.mapping",conn)

    # Get just the student ids and concatenate with the label_years 
    student_ids = pd.DataFrame(ids[~ids['person_id'].isin(label_years_final['person_id'])]['person_id'])
    labels = pd.concat([student_ids,label_years_final])
    labels = labels.fillna(0)

    # Fix issues with some labels being multi-class
    for column in labels.columns:
        if column=='person_id':
            continue
        labels.loc[labels[column]!=0,column] = 1

    print('Writing to database')

    # Write to database 
    labels.to_sql('labels', con=conn, schema='training', if_exists = 'replace')
    
    print('Finished writing labels')
    
    ###generate a first_year_interaction column in the labels table
    sql = '''create table training.labels2 as  (select a.*,  case when year_2009 > 0 then 2009 
     when year_2010 > 0 then 2010  
     when year_2011 > 0 then 2011 
     when year_2012 > 0 then 2012 
     when year_2013 > 0 then 2013 
     when year_2014 > 0 then 2014 
     when year_2015 > 0 then 2015 
     when year_2016 > 0 then 2016 else               
     10000 end as first_year_interaction 
     from training.labels a);
     
     drop table training.labels;
     alter table training.labels2 RENAME TO labels
     '''
    conn.execute(sql)
    
    sql_wide_window = '''create table training.labels_widerwindow as ( SELECT *, case when 1 
    IN(year_2009, year_2010, year_2011, year_2012, year_2013, year_2014, year_2015, 
    year_2016) then 1 else 0 end as ever_interacted, 
    case when 1 in (year_2009, year_2010) then 1 else 0
    end as year_0910, 
    case when 1 in (year_2010, year_2011) then 1 else 0
    end as year_1011, 
    case when 1 in (year_2011, year_2012) then 1 else 0
    end as year_1112, 
    case when 1 in (year_2012, year_2013) then 1 else 0
    end as year_1213, 
    case when 1 in (year_2013, year_2014) then 1 else 0
    end as year_1314, 
    case when 1 in (year_2014, year_2015) then 1 else 0
    end as year_1415,
    case when 1 in (year_2015, year_2016) then 1 else 0
    end as year_1516,
    case when 1 in (year_2009, year_2010, year_2011) then 1 else 0
    end as year_091011,
    case when 1 in (year_2010, year_2011, year_2012) then 1 else 0
    end as year_101112,
    case when 1 in (year_2011, year_2012, year_2013) then 1 else 0
    end as year_111213,
    case when 1 in (year_2012, year_2013, year_2014) then 1 else 0
    end as year_121314,
    case when 1 in (year_2013, year_2014, year_2015) then 1 else 0
    end as year_131415,
    case when 1 in (year_2014, year_2015, year_2016) then 1 else 0
    end as year_141516,
    case when 1 in (year_2009, year_2010, year_2011, year_2012, year_2013) then 1 else 0
    end as year_09to13,
    case when 1 in (year_2010, year_2011, year_2012, year_2013, year_2014) then 1 else 0
    end as year_10to14,
    case when 1 in (year_2011, year_2012, year_2013, year_2014, year_2015) then 1 else 0
    end as year_11to15,
    case when 1 in (year_2012, year_2013, year_2014, year_2015, year_2016) then 1 else 0
    end as year_12to16 FROM training.labels );
    
    drop table training.labels;
    alter table training.labels_widerwindow rename to labels;'''
    
    conn.execute(sql_wide_window)




