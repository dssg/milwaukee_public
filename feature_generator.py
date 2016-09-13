import pandas as pd 
import re 
import preprocessing as pp
from abstractfeature import AbstractFeature
import abc
from abstractfeature import (AbstractAssessmentFeature, AbstractAssessmentSlope,
                             AbstractDisciplineFeature, AbstractAssessmentAggregates,
                             AbstractFeature)


class is_student_relevant(AbstractFeature):
    
    def __init__(self, table_name, conn):
        super(is_student_relevant, self).__init__(table_name, conn)    
        
    @property
    def feature_id(self):
        return -1 
    @property
    def feature_col(self):
        return 'is_student_relevant' 
    @property
    def feature_type(self):  
        return 'boolean'
    @property
    def feature_description(self):
        return '''check if the student should exist in this table, based on the
                  given year. Student must be at most 19 years old to be included'''
        
    def feature_code(self):
        rf_type, timeframe = self.extract_timeframe_from_table_name() 
        if rf_type == 'year':
          sql = '''SELECT person_id, case when person_id is not null then 1 else 0 end as is_student_relevant 
                   from (SELECT person_id
                   FROM edu_schema.demographic a
                   JOIN training.mapping b ON a.student_key = b.student_key
                   where year <= {timeframe} and {timeframe} - birth_year < 20
                   group by 1
                 ) as t '''.format(timeframe=timeframe)

        data = pd.read_sql_query(sql, self.conn)
        return data, "is_student_relevant"


class first_dis_age(AbstractFeature):
    
    def __init__(self, table_name, conn):
        super(first_dis_age, self).__init__(table_name, conn)    
        
    @property
    def feature_id(self):
        return 10 
    @property
    def feature_col(self):
        return 'first_dis_age' 
    @property
    def feature_type(self):  
        return 'numerical'
    @property
    def feature_description(self):
        return '''student age for the first discipline incient. calculated by incident_year - birth_year. In case that first discipline doesnt exist, the feature get default age of 1000'''
        
    def feature_code(self):
        ''' function should get the features table name, the current feature name and 
        calculate the feature and insert it to the table 
        '''
        #getting timeframe and time type (age/year )
        rf_type, timeframe = self.extract_timeframe_from_table_name() 
 
        disc = pd.read_sql_query("SELECT student_key, discipline_start_date FROM edu_schema.discipline;", self.conn)
        disc['discipline_year'] = disc['discipline_start_date'].astype('datetime64[ns]').dt.year

        if rf_type == 'year':
            disc = disc[disc['discipline_year'] <= timeframe]

        demo = pd.read_sql_query("SELECT student_key, student_birthdate, year FROM edu_schema.demographic;", self.conn)
        demo['birthyear'] = demo['student_birthdate'].astype('datetime64[ns]').dt.year

        first_dis_age = pd.merge(demo, disc, how = 'left', on ='student_key')
        first_dis_age['first_dis_age'] = first_dis_age['discipline_year'] - first_dis_age['birthyear']
        #get only the first age 
        first_dis_age = first_dis_age.groupby(['student_key'])['first_dis_age'].min()
        age_df = pd.DataFrame({'student_key':first_dis_age.index, 'first_dis_age':first_dis_age.values})
        #get person_id 
        person_id = pd.read_sql_query("SELECT person_id ,student_key FROM training.mapping;", self.conn)
        first_dis_age_id = pd.merge(person_id, age_df , how = 'left', on = 'student_key')
        #dropping all the other columns except the new feature and person_id 
        first_dis_age_id = first_dis_age_id[['person_id','first_dis_age']]
        #checking desired timeframe - if first discipline incident happened *after* timeframe - drop it  
        if rf_type == 'age': 
            feature_data = first_dis_age_id[first_dis_age_id['first_dis_age'] <= timeframe]
        else: 
            feature_data = first_dis_age_id
        # return a df with person_id, feature_value and feature name
        #preprocess --- replace negative values with average value 
        age_mean = feature_data.loc[feature_data[self.feature_col], 'first_dis_age'].mean()
        feature_data.loc[feature_data[self.feature_col] < 0, 'age'] = age_mean
        feature_data = pp.fill_null_with_default_value(feature_data, 'first_dis_age', 100)
        return feature_data, 'first_dis_age' 


class birth_month(AbstractFeature):
    def __init__(self, table_name, conn):
        super(birth_month, self).__init__(table_name, conn)    
        
    @property
    def feature_id(self):
        return 11 

    @property
    def feature_col(self):
        return 'birth_month' 

    @property
    def feature_type(self):  
        return 'numerical'

    @property
    def feature_description(self):
        return '''students' birth month '''
        
    def feature_code(self):
        ''' function should get the features table name, the current feature name and 
        calculate the feature and insert it to the table 
        '''
        sql = '''SELECT person_id, extract(month from student_birthdate) as birth_month
                   FROM edu_schema.demographic a
                   RIGHT JOIN training.mapping b ON a.student_key = b.student_key
                   GROUP BY 1,2'''
        data = pd.read_sql_query(sql,self.conn)
        data = data.drop_duplicates()
        data = pp.fill_null_with_default_value(data, "birth_month", 8)
        return data, "birth_month"


class num_demo_records(AbstractFeature):
    def __init__(self, table_name, conn):
        super(num_demo_records, self).__init__(table_name, conn)    
        
    @property
    def feature_id(self):
        return 12

    @property
    def feature_col(self):
        return 'num_demo_records' 

    @property
    def feature_type(self):  
        return 'numerical'

    @property
    def feature_description(self):
        return '''students' overall number of demogaphics records'''
        
    def feature_code(self):
        ''' function should get the features table name, the current feature name and 
        calculate the feature and insert it to the table 
        '''
        rf_type, timeframe = self.extract_timeframe_from_table_name() 
        if rf_type == 'year': 
            sql = '''SELECT person_id, num_demo_records FROM (
                        SELECT a.student_key, person_id, count(*) as num_demo_records 
                           FROM edu_schema.demographic a
                               RIGHT JOIN training.mapping b ON a.student_key = b.student_key
                                  AND year <= {}
                                    group by 1,2) as t'''.format(timeframe)
        else: 
            sql = '''SELECT person_id, num_demo_records FROM (
                        SELECT a.student_key, person_id, count(*) as num_demo_records 
                           FROM edu_schema.demographic a
                               RIGHT JOIN training.mapping b ON a.student_key = b.student_key
                                  AND  year - extract(year from student_birthdate) <= {}
                                    group by 1,2) as t'''.format(timeframe)            
        data = pd.read_sql_query(sql, self.conn)
        return data, "num_demo_records"


class demo_records_per_year(AbstractFeature):
    def __init__(self, table_name, conn):
        super(demo_records_per_year, self).__init__(table_name, conn)    
        
    @property
    def feature_id(self):
        return 13

    @property
    def feature_col(self):
        return 'demo_records_per_year' 

    @property
    def feature_type(self):  
        return 'numerical'

    @property
    def feature_description(self):
        return '''students' average number of demogaphics records per year'''
        
    def feature_code(self):
        ''' function should get the features table name, the current feature name and 
        calculate the feature and insert it to the table 
        '''
        rf_type, timeframe = self.extract_timeframe_from_table_name() 
        if rf_type == 'year': 
            sql = '''  SELECT person_id, 
                        CASE WHEN num_years > 0 then cast(num_records as float) / num_years ELSE 0 end as demo_records_per_year 
                             from (SELECT 
                                   a.student_key,
                                      b.person_id, 
                                         count(*) as num_records, 
                                           count(distinct school_year) as num_years
                             FROM edu_schema.demographic a
                             right JOIN training.mapping b ON a.student_key = b.student_key
                             AND year <= {}
                             group by 1,2) as t'''.format(timeframe)
            
        data = pd.read_sql_query(sql, self.conn)
        return data, "demo_records_per_year"


class max_demo_records_per_year(AbstractFeature):
    def __init__(self, table_name, conn):
        super(max_demo_records_per_year, self).__init__(table_name, conn)    
        
    @property
    def feature_id(self):
        return 19

    @property
    def feature_col(self):
        return 'max_demo_records_per_year' 

    @property
    def feature_type(self):  
        return 'numerical'

    @property
    def feature_description(self):
        return '''students' maximum number of demogaphics records per year'''
        
    def feature_code(self):
        ''' function should get the features table name, the current feature name and 
        calculate the feature and insert it to the table 
        '''
        rf_type, timeframe = self.extract_timeframe_from_table_name() 
        if rf_type == 'year': 
            sql = '''SELECT person_id, 
                        max(num_records) as max_demo_records_per_year 
                             from (SELECT 
                                   a.student_key,
                                      b.person_id, 
                                       a.school_year,
                                         count(*) as num_records 
                             FROM edu_schema.demographic a
                             right JOIN training.mapping b ON a.student_key = b.student_key
                             AND year <= {}
                             group by 1,2,3) as t
                             group by 1'''.format(timeframe)
        else: 
            sql = '''SELECT person_id, 
                        max(num_records) as max_demo_records_per_year 
                             from (SELECT 
                                   a.student_key,
                                      b.person_id, 
                                       a.school_year,
                                         count(*) as num_records 
                             FROM edu_schema.demographic a
                             right JOIN training.mapping b ON a.student_key = b.student_key
                             AND  year - extract(year from student_birthdate)  <= {}
                             group by 1,2,3) as t
                             group by 1'''.format(timeframe) 
        data = pd.read_sql_query(sql, self.conn)
        return data, "max_demo_records_per_year"


class avg_att_days_per_year(AbstractFeature):
    def __init__(self, table_name, conn):
        super(avg_att_days_per_year, self).__init__(table_name, conn)    
        
    @property
    def feature_id(self):
        return 14

    @property
    def feature_col(self):
        return 'avg_att_days_per_year' 

    @property
    def feature_type(self):  
        return 'numerical'

    @property
    def feature_description(self):
        return '''students' average number of attendance days per year'''
        
    def feature_code(self):
        ''' function should get the features table name, the current feature name and 
        calculate the feature and insert it to the table 
        '''
        rf_type, timeframe = self.extract_timeframe_from_table_name() 
        if rf_type == 'year': 

            sql = '''  SELECT person_id, avg_att_days_per_year from (
                          select person_id, a.student_key , avg(att_days) as avg_att_days_per_year
                             from edu_schema.demographic d
                             left join edu_schema.attendance a ON a.student_key = d.student_key
                                 right JOIN training.mapping b ON a.student_key = b.student_key
                                   AND a.year <= {}   group by 1,2) as t'''.format(timeframe)
        else: 
            sql = '''  SELECT person_id, avg_att_days_per_year from (
                          select person_id, a.student_key , avg(att_days) as avg_att_days_per_year
                             from edu_schema.demographic a
                                    right JOIN training.mapping b ON a.student_key = b.student_key
                                     left join edu_schema.attendance c ON a.student_key = c.student_key 
                                   AND a.year - extract(year from student_birthdate) <= {}   
                                       group by 1,2) as t'''.format(timeframe)
        data = pd.read_sql_query(sql, self.conn)
        data = pp.fill_null_with_mean(data, "avg_att_days_per_year")
        return data, "avg_att_days_per_year"



class avg_abs_days_per_year(AbstractFeature): 
    def __init__(self, table_name, conn):
        super(avg_abs_days_per_year, self).__init__(table_name, conn)    
        
    @property
    def feature_id(self):
        return 15

    @property
    def feature_col(self):
        return 'avg_abs_days_per_year' 

    @property
    def feature_type(self):  
        return 'numerical'

    @property
    def feature_description(self):
        return '''students' average number of absence days per year'''
        
    def feature_code(self):
        ''' function should get the features table name, the current feature name and 
        calculate the feature and insert it to the table 
        '''
        rf_type, timeframe = self.extract_timeframe_from_table_name() 
        if rf_type == 'year': 

            sql = '''  SELECT person_id, avg_abs_days_per_year from (
                          select person_id, a.student_key , avg(total_membership_days - att_days) as avg_abs_days_per_year
                             from edu_schema.attendance a
                                right JOIN training.mapping b ON a.student_key = b.student_key
                                   AND a.year <= {}   group by 1,2) as t'''.format(timeframe)
        else: 
            sql = '''  SELECT person_id, avg_abs_days_per_year from (
                          select person_id, a.student_key , avg(total_membership_days - att_days) as avg_abs_days_per_year
                             from edu_schema.attendance a
                                right  JOIN training.mapping b ON a.student_key = b.student_key
                                      left join edu_schema.demographic c ON a.student_key = c.student_key 
                                   AND a.year - extract(year from student_birthdate) <= {}   
                                       group by 1,2) as t'''.format(timeframe)
        data = pd.read_sql_query(sql, self.conn)
        data = pp.fill_null_with_mean(data, "avg_abs_days_per_year")
        return data, "avg_abs_days_per_year"




class avg_membership_days_per_year(AbstractFeature): 
    def __init__(self, table_name, conn):
        super(avg_membership_days_per_year, self).__init__(table_name, conn)    
        
    @property
    def feature_id(self):
        return 16

    @property
    def feature_col(self):
        return 'avg_membership_days_per_year' 

    @property
    def feature_type(self):  
        return 'numerical'

    @property
    def feature_description(self):
        return '''students' average number of memberships days per year'''
        
    def feature_code(self):
        ''' function should get the features table name, the current feature name and 
        calculate the feature and insert it to the table 
        '''
        rf_type, timeframe = self.extract_timeframe_from_table_name() 
        if rf_type == 'year': 
            sql = '''  SELECT person_id, avg_membership_days_per_year from (
                          select person_id, a.student_key , avg(total_membership_days) as avg_membership_days_per_year
                             from edu_schema.attendance a
                                  right JOIN training.mapping b ON a.student_key = b.student_key
                                   AND a.year <= {}   group by 1,2) as t'''.format(timeframe)

        data = pd.read_sql_query(sql, self.conn)
        data = pp.fill_null_with_mean(data, "avg_membership_days_per_year")
        return data, "avg_membership_days_per_year"




class att_variance_over_years(AbstractFeature): 
    def __init__(self, table_name, conn):
        super(att_variance_over_years, self).__init__(table_name, conn)    
        
    @property
    def feature_id(self):
        return 17

    @property
    def feature_col(self):
        return 'att_variance_over_years' 

    @property
    def feature_type(self):  
        return 'numerical'

    @property
    def feature_description(self):
        return '''students' variance of attendance days over the years'''
        
    def feature_code(self):
        ''' function should get the features table name, the current feature name and 
        calculate the feature and insert it to the table 
        '''
        rf_type, timeframe = self.extract_timeframe_from_table_name() 
        if rf_type == 'year': 
            sql = '''  SELECT person_id, att_variance_over_years from (
                          select person_id, a.student_key , variance(att_days) as att_variance_over_years
                             from edu_schema.attendance a
                                  right JOIN training.mapping b ON a.student_key = b.student_key
                                   AND a.year <= {}   group by 1,2) as t'''.format(timeframe)
        else: 
            sql = '''  SELECT person_id, att_variance_over_years from (
                          select person_id, a.student_key , variance(att_days) as att_variance_over_years
                             from edu_schema.attendance a
                                right JOIN training.mapping b ON a.student_key = b.student_key
                                   left  join edu_schema.demographic c ON a.student_key = c.student_key 
                                   AND a.year - extract(year from student_birthdate) <= {}   
                                       group by 1,2) as t'''.format(timeframe)
        data = pd.read_sql_query(sql, self.conn)
        data = pp.fill_null_with_mean(data, "att_variance_over_years")
        return data, "att_variance_over_years"




class min_max_att_ratio(AbstractFeature):
    def __init__(self, table_name, conn):
        super(min_max_att_ratio, self).__init__(table_name, conn)    
        
    @property
    def feature_id(self):
        return 18

    @property
    def feature_col(self):
        return 'min_max_att_ratio' 

    @property
    def feature_type(self):  
        return 'numerical'

    @property
    def feature_description(self):
        return '''students' ratio between max(att_days_per_year) and min(att_days_per_year). if min is zero, it replaced with 0.1'''
        
    def feature_code(self):
        ''' function should get the features table name, the current feature name and 
        calculate the feature and insert it to the table 
        '''
        rf_type, timeframe = self.extract_timeframe_from_table_name() 
        if rf_type == 'year': 
            sql = '''SELECT person_id, (max_att_days / min_att_days) as min_max_att_ratio from (
                          select  person_id, a.student_key , MAX(att_days) as max_att_days,
                          case when MIN(cast(att_days as float))=0 then 0.1 else MIN(cast(att_days as float)) end as min_att_days
                             from edu_schema.attendance a
                                right JOIN training.mapping b ON a.student_key = b.student_key
                                   AND a.year <= {}   group by 1,2) as t'''.format(timeframe)
        data = pd.read_sql_query(sql, self.conn)
        data = pp.fill_null_with_mean(data, "min_max_att_ratio")
        return data, "min_max_att_ratio"


class first_last_att_gap(AbstractFeature):
    def __init__(self, table_name, conn):
        super(first_last_att_gap, self).__init__(table_name, conn)    
        
    @property
    def feature_id(self):
        return 19

    @property
    def feature_col(self):
        return 'first_last_att_gap' 

    @property
    def feature_type(self):  
        return 'numerical'

    @property
    def feature_description(self):
        return '''students' gap between attendance days in first year and in last year of calculation '''
        
    def feature_code(self):
        ''' function should get the features table name, the current feature name and 
        calculate the feature and insert it to the table 
        '''
        rf_type, timeframe = self.extract_timeframe_from_table_name() 
        if rf_type == 'year': 
            sql = '''                SELECT person_id, last_att_days - first_att_days as first_last_att_gap from (       
                            select student_key, att_days as last_att_days from (select student_key, att_days, year,
                            row_number() OVER (PARTITION BY a.student_key ORDER BY a.year desc) as rank1,
                            count(*) over (PARTITION BY a.student_key) as total_count
                             from edu_schema.attendance a) as s
                             where rank1 = total_count and year <= {}) as a 
                             join 
                             (select student_key, att_days as first_att_days 
                             from (select student_key, att_days, year,
                            row_number() OVER (PARTITION BY a.student_key ORDER BY a.year desc) as rank1
                             from edu_schema.attendance a) as t
                             where rank1=1 and year <= {}) as b on a.student_key = b.student_key
                             right join training.mapping d on a.student_key = d.student_key '''.format(timeframe,timeframe)
        else: 
            sql = ''' SELECT person_id, last_att_days - first_att_days as first_last_att_gap from (       
                            select student_key, att_days as last_att_days from (select a.student_key, att_days, a.year, student_birthdate,
                            row_number() OVER (PARTITION BY a.student_key ORDER BY a.year desc) as rank1,
                            count(*) over (PARTITION BY a.student_key) as total_count
                             from edu_schema.attendance a
                             join edu_schema.demographic demo on a.student_key = demo.student_key) as s
                             where rank1 = total_count and year - extract(year from student_birthdate) <= {}) as a 
                             join 
                             (select student_key, att_days as first_att_days 
                             from (select a.student_key, att_days, a.year, student_birthdate,
                            row_number() OVER (PARTITION BY a.student_key ORDER BY a.year desc) as rank1
                             from edu_schema.attendance a
                             join edu_schema.demographic demo on a.student_key = demo.student_key) as t
                             where rank1=1 and year - extract(year from student_birthdate) <= {}) as b on a.student_key = b.student_key
                             right join training.mapping d on a.student_key = d.student_key'''.format(timeframe,timeframe)
        data = pd.read_sql_query(sql, self.conn)
        #demographic = pd.read_sql_query("SELECT (distinct student_key) FROM edu_schema.demographic ")
        #merged = pd.merge(demographic, data, how =left , on = student_key)
        #merged = merged [['person_id', 'first_last_att_gap']]
        data = pp.fill_null_with_mean(data, "first_last_att_gap")
        return data, "first_last_att_gap"


class att_days_last_year(AbstractFeature):
    def __init__(self, table_name, conn):
        super(att_days_last_year, self).__init__(table_name, conn)    
        
    @property
    def feature_id(self):
        return 18

    @property
    def feature_col(self):
        return 'att_days_last_year' 

    @property
    def feature_type(self):  
        return 'numerical'

    @property
    def feature_description(self):
        return '''students' number of attendance days on last year '''
        
    def feature_code(self):
        ''' function should get the features table name, the current feature name and 
        calculate the feature and insert it to the table 
        '''
        rf_type, timeframe = self.extract_timeframe_from_table_name() 
        if rf_type == 'year': 
            sql = '''SELECT person_id, att_days as att_days_last_year
                             from edu_schema.attendance a
                                right JOIN training.mapping b ON a.student_key = b.student_key
                                   AND a.year = {}'''.format(timeframe)
        data = pd.read_sql_query(sql, self.conn)
        data = pp.fill_null_with_mean(data, "att_days_last_year")
        return data, "att_days_last_year"

class num_of_incidents_learning_environment(AbstractFeature):
    
    def __init__(self, table_name, conn):
        super(num_of_incidents_learning_environment, self).__init__(table_name, conn)    
        
    @property
    def feature_id(self):
        return 23 

    @property
    def feature_col(self):
        return 'num_of_incidents_learning_environment' 

    @property
    def feature_type(self):  
        return 'numerical'

    @property
    def feature_description(self):
        return 'Total number of discipline incidents categorized as learning environment'
        
    def feature_code(self):
        
        #get discipline
        disc = pd.read_sql_query("SELECT student_key, discipline_start_date FROM edu_schema.discipline where discipline_fed_offense_group = 'Learning Environment'", self.conn)
        #extract discipline year 
        disc['discipline_year'] = disc['discipline_start_date'].astype('datetime64[ns]').dt.year
        disc = disc[['student_key','discipline_year']]

        #get person_id 
        ids = pd.read_sql_query("SELECT person_id ,student_key FROM training.mapping", self.conn)
        
        #get mode and threshold 
        rf_type, timeframe = self.extract_timeframe_from_table_name() 

        if rf_type == 'year':
            #keep only the pre-timeframe data
            disc_year = disc[disc['discipline_year'] <= timeframe]
            #group by student_key 
            d = disc_year.groupby(disc_year['student_key'])['discipline_year'].count()
            df = pd.DataFrame({'student_key':d.index, 'count':d.values})
            
        elif rf_type == 'age':
            #get demo 
            demo = pd.read_sql_query("SELECT student_key, student_birthdate FROM edu_schema.demographic", self.conn)
            #get birth_year
            demo['birth_year'] = demo['student_birthdate'].astype('datetime64[ns]').dt.year
            demo = demo[['student_key','birth_year']]
            #merge with discipline on student_key
            df = pd.merge(demo, disc, on = 'student_key')
            #calculate age at discipline
            df['age_at_discipline'] = df['discipline_year'] - df['birth_year'] 
            #keep only the pre-timeframe data
            df = df[df['age_at_discipline'] <= timeframe]
            df = df[['student_key','age_at_discipline']]
            #group by student_key 
            d = df.groupby(df['student_key'])['age_at_discipline'].count()
            df = pd.DataFrame({'student_key':d.index, 'count':d.values})
            
        #merge with ids on student_key
        feature_data = pd.merge(ids, df, on = 'student_key', how = 'left')
        feature_data = feature_data[['person_id','count']]
        feature_data.rename(columns={'count': self.feature_col}, inplace = True)
        #replace nulls with 0
        feature_data = pp.fill_null_with_default_value(feature_data, self.feature_col, 0)
        print (feature_data.shape)
        return feature_data, 'num_of_incidents_learning_environment'


class num_of_incidents_physical_safety(AbstractFeature):
    
    def __init__(self, table_name, conn):
        super(num_of_incidents_physical_safety, self).__init__(table_name, conn)    
        
    @property
    def feature_id(self):
        return 24 
    @property
    def feature_col(self):
        return 'num_of_incidents_physical_safety' 
    @property
    def feature_type(self):  
        return 'numerical'
    @property
    def feature_description(self):
        return 'Total number of discipline incidents categorized as physical/personal safety'
        
    def feature_code(self):
        
        #get discipline
        disc = pd.read_sql_query("SELECT student_key, discipline_start_date FROM edu_schema.discipline where discipline_fed_offense_group = 'Personal/Physical Safety'", self.conn)
        #extract discipline year 
        disc['discipline_year'] = disc['discipline_start_date'].astype('datetime64[ns]').dt.year
        disc = disc[['student_key','discipline_year']]

        #get person_id 
        ids = pd.read_sql_query("SELECT person_id ,student_key FROM training.mapping", self.conn)
        
        #get mode and threshold 
        rf_type, timeframe = self.extract_timeframe_from_table_name() 

        if rf_type == 'year':
            #keep only the pre-timeframe data
            disc_year = disc[disc['discipline_year'] <= timeframe]
            #group by student_key 
            d = disc_year.groupby(disc_year['student_key'])['discipline_year'].count()
            df = pd.DataFrame({'student_key':d.index, 'count':d.values})
            
        elif rf_type == 'age':
            #get demo 
            demo = pd.read_sql_query("SELECT student_key, student_birthdate FROM edu_schema.demographic", self.conn)
            #get birth_year
            demo['birth_year'] = demo['student_birthdate'].astype('datetime64[ns]').dt.year
            demo = demo[['student_key','birth_year']]
            #merge with discipline on student_key
            df = pd.merge(demo, disc, on = 'student_key')
            #calculate age at discipline
            df['age_at_discipline'] = df['discipline_year'] - df['birth_year'] 
            #keep only the pre-timeframe data
            df = df[df['age_at_discipline'] <= timeframe]
            df = df[['student_key','age_at_discipline']]
            #group by student_key 
            d = df.groupby(df['student_key'])['age_at_discipline'].count()
            df = pd.DataFrame({'student_key':d.index, 'count':d.values})
            
        #merge with ids on student_key
        feature_data = pd.merge(ids, df, on = 'student_key', how = 'left')
        feature_data = feature_data[['person_id','count']]
        feature_data.rename(columns={'count': self.feature_col}, inplace = True)
        #replace nulls with 0
        feature_data = pp.fill_null_with_default_value(feature_data, self.feature_col, 0)
       
        return feature_data, 'num_of_incidents_physical_safety'
        
#######################################################

class num_of_incidents_weapons_related(AbstractFeature):
    
    def __init__(self, table_name, conn):
        super(num_of_incidents_weapons_related, self).__init__(table_name, conn)    
        
    @property
    def feature_id(self):
        return 25 
    @property
    def feature_col(self):
        return 'num_of_incidents_weapons_related' 
    @property
    def feature_type(self):  
        return 'numerical'
    @property
    def feature_description(self):
        return 'Total number of discipline incidents categorized as weapon related'
        
    def feature_code(self):
        
        #get discipline
        disc = pd.read_sql_query("SELECT student_key, discipline_start_date FROM edu_schema.discipline where discipline_fed_offense_group = 'Weapons'", self.conn)
        #extract discipline year 
        disc['discipline_year'] = disc['discipline_start_date'].astype('datetime64[ns]').dt.year
        disc = disc[['student_key','discipline_year']]

        #get person_id 
        ids = pd.read_sql_query("SELECT person_id ,student_key FROM training.mapping", self.conn)
        
        #get mode and threshold 
        rf_type, timeframe = self.extract_timeframe_from_table_name() 

        if rf_type == 'year':
            #keep only the pre-timeframe data
            disc_year = disc[disc['discipline_year'] <= timeframe]
            #group by student_key 
            d = disc_year.groupby(disc_year['student_key'])['discipline_year'].count()
            df = pd.DataFrame({'student_key':d.index, 'count':d.values})
            
        elif rf_type == 'age':
            #get demo 
            demo = pd.read_sql_query("SELECT student_key, student_birthdate FROM edu_schema.demographic", self.conn)
            #get birth_year
            demo['birth_year'] = demo['student_birthdate'].astype('datetime64[ns]').dt.year
            demo = demo[['student_key','birth_year']]
            #merge with discipline on student_key
            df = pd.merge(demo, disc, on = 'student_key')
            #calculate age at discipline
            df['age_at_discipline'] = df['discipline_year'] - df['birth_year'] 
            #keep only the pre-timeframe data
            df = df[df['age_at_discipline'] <= timeframe]
            df = df[['student_key','age_at_discipline']]
            #group by student_key 
            d = df.groupby(df['student_key'])['age_at_discipline'].count()
            df = pd.DataFrame({'student_key':d.index, 'count':d.values})
            
        #merge with ids on student_key
        feature_data = pd.merge(ids, df, on = 'student_key', how = 'left')
        feature_data = feature_data[['person_id','count']]
        feature_data.rename(columns={'count': self.feature_col}, inplace = True)
        #replace nulls with 0
        feature_data = pp.fill_null_with_default_value(feature_data, self.feature_col, 0)
       
        return feature_data, 'num_of_incidents_weapons_related'
    
##################################################

class has_drug_related_discipline(AbstractFeature):
    
    def __init__(self, table_name, conn):
        super(has_drug_related_discipline, self).__init__(table_name, conn)    
        
    @property
    def feature_id(self):
        return 26 
    @property
    def feature_col(self):
        return 'has_drug_related_discipline' 
    @property
    def feature_type(self):  
        return 'boolean'
    @property
    def feature_description(self):
        return 'Determines whether or not the student has had drug related discipline incidents'
        
    def feature_code(self):
        
        #get discipline
        disc = pd.read_sql_query("SELECT student_key, discipline_start_date FROM edu_schema.discipline where discipline_offense_type = 'Possession/Ownership/Use of Drugs'", self.conn)
        #extract discipline year 
        disc['discipline_year'] = disc['discipline_start_date'].astype('datetime64[ns]').dt.year
        disc = disc[['student_key','discipline_year']]

        #get person_id 
        ids = pd.read_sql_query("SELECT person_id ,student_key FROM training.mapping", self.conn)
        
        #get mode and threshold 
        rf_type, timeframe = self.extract_timeframe_from_table_name() 

        if rf_type == 'year':
            #keep only the pre-timeframe data
            disc_year = disc[disc['discipline_year'] <= timeframe]
            #group by student_key 
            d = disc_year.groupby(disc_year['student_key'])['discipline_year'].count()
            df = pd.DataFrame({'student_key':d.index, 'count':d.values})
            
        elif rf_type == 'age':
            #get demo 
            demo = pd.read_sql_query("SELECT student_key, student_birthdate FROM edu_schema.demographic", self.conn)
            #get birth_year
            demo['birth_year'] = demo['student_birthdate'].astype('datetime64[ns]').dt.year
            demo = demo[['student_key','birth_year']]
            #merge with discipline on student_key
            df = pd.merge(demo, disc, on = 'student_key')
            #calculate age at discipline
            df['age_at_discipline'] = df['discipline_year'] - df['birth_year'] 
            #keep only the pre-timeframe data
            df = df[df['age_at_discipline'] <= timeframe]
            df = df[['student_key','age_at_discipline']]
            #group by student_key 
            d = df.groupby(df['student_key'])['age_at_discipline'].count()
            df = pd.DataFrame({'student_key':d.index, 'count':d.values})
            
        #merge with ids on student_key
        feature_data = pd.merge(ids, df, on = 'student_key', how = 'left')
        feature_data = feature_data[['person_id','count']]
        #replace nulls with 0
        feature_data = pp.fill_null_with_default_value(feature_data, 'count', 0)
        #replace non-zero values with 1
        feature_data['count'] = (feature_data['count']>0).apply(int)
        feature_data.rename(columns={'count': self.feature_col}, inplace = True)    
       
        return feature_data, 'has_drug_related_discipline' 
    
##################################################

class num_discipline_days(AbstractFeature):
    
    def __init__(self, table_name, conn):
        super(num_discipline_days, self).__init__(table_name, conn)    
        
    @property
    def feature_id(self):
        return 27 
    @property
    def feature_col(self):
        return 'num_discipline_days' 
    @property
    def feature_type(self):  
        return 'numerical'
    @property
    def feature_description(self):
        return 'Total number of discipline days'
        
    def feature_code(self):
        
        #get discipline
        disc = pd.read_sql_query("SELECT student_key, discipline_start_date, discipline_days FROM edu_schema.discipline", self.conn)
        #extract discipline year 
        disc['discipline_year'] = disc['discipline_start_date'].astype('datetime64[ns]').dt.year
        disc = disc[['student_key','discipline_year','discipline_days']]

        #get person_id 
        ids = pd.read_sql_query("SELECT person_id, student_key FROM training.mapping", self.conn)

        #get mode and threshold 
        rf_type, timeframe = self.extract_timeframe_from_table_name() 

        if rf_type == 'year':
            #keep only the pre-timeframe data
            disc_year = disc[disc['discipline_year'] <= timeframe]
            #group by student_key 
            d = disc_year.groupby(disc_year['student_key'])['discipline_days'].sum()
            df = pd.DataFrame({'student_key':d.index, 'count':d.values})

        elif rf_type == 'age':
            #get demo 
            demo = pd.read_sql_query("SELECT student_key, student_birthdate FROM edu_schema.demographic", self.conn)
            #get birth_year
            demo['birth_year'] = demo['student_birthdate'].astype('datetime64[ns]').dt.year
            demo = demo[['student_key','birth_year']]
            #merge with discipline on student_key
            df = pd.merge(demo, disc, on = 'student_key')
            #calculate age at discipline
            df['age_at_discipline'] = df['discipline_year'] - df['birth_year'] 
            #keep only the pre-timeframe data
            df = df[df['age_at_discipline'] <= timeframe]
            df = df[['student_key','discipline_days']]
            #group by student_key 
            d = df.groupby(df['student_key'])['discipline_days'].sum()
            df = pd.DataFrame({'student_key':d.index, 'count':d.values})

        #merge with ids on student_key
        feature_data = pd.merge(ids, df, on = 'student_key', how = 'left')
        feature_data = feature_data[['person_id','count']]
        #replace nulls with 0
        feature_data = pp.fill_null_with_default_value(feature_data, 'count', 0)
        feature_data.rename(columns={'count': self.feature_col}, inplace = True)      

        return feature_data, 'num_discipline_days'
    
###########################################

class discipline_days_zero_low_medium_high(AbstractFeature):
    
    def __init__(self, table_name, conn):
        super(discipline_days_zero_low_medium_high, self).__init__(table_name, conn)    
        
    @property
    def feature_id(self):
        return 28 
    @property
    def feature_col(self):
        return 'discipline_days_zero_low_medium_high' 
    @property
    def feature_type(self):  
        return 'categorical'
        
    @property
    def feature_description(self):
        return 'Quantizing number of discipline days into four categories: {Zero: 0, Low: 1-5, Medium: 6-20, High: >20}'

    def feature_code(self):
        
        #get discipline
        disc = pd.read_sql_query("SELECT student_key, discipline_start_date, discipline_days FROM edu_schema.discipline", self.conn)
        #extract discipline year 
        disc['discipline_year'] = disc['discipline_start_date'].astype('datetime64[ns]').dt.year
        disc = disc[['student_key','discipline_year','discipline_days']]

        #get person_id 
        ids = pd.read_sql_query("SELECT person_id, student_key FROM training.mapping", self.conn)

        #get mode and threshold 
        rf_type, timeframe = self.extract_timeframe_from_table_name() 

        if rf_type == 'year':
            #keep only the pre-timeframe data
            disc_year = disc[disc['discipline_year'] <= timeframe]
            #group by student_key 
            d = disc_year.groupby(disc_year['student_key'])['discipline_days'].sum()
            df = pd.DataFrame({'student_key':d.index, 'count':d.values})

        elif rf_type == 'age':
            #get demo 
            demo = pd.read_sql_query("SELECT student_key, student_birthdate FROM edu_schema.demographic", self.conn)
            #get birth_year
            demo['birth_year'] = demo['student_birthdate'].astype('datetime64[ns]').dt.year
            demo = demo[['student_key','birth_year']]
            #merge with discipline on student_key
            df = pd.merge(demo, disc, on = 'student_key')
            #calculate age at discipline
            df['age_at_discipline'] = df['discipline_year'] - df['birth_year'] 
            #keep only the pre-timeframe data
            df = df[df['age_at_discipline'] <= timeframe]
            df = df[['student_key','discipline_days']]
            #group by student_key 
            d = df.groupby(df['student_key'])['discipline_days'].sum()
            df = pd.DataFrame({'student_key':d.index, 'count':d.values})

        #merge with ids on student_key
        feature_data = pd.merge(ids, df, on = 'student_key', how = 'left')
        feature_data = feature_data[['person_id','count']]
        #replace nulls with 0
        feature_data = pp.fill_null_with_default_value(feature_data, 'count', 0)
        #quantizition (binning)
        bins = [-1,0,5,20,100000000]
        group_names = ['Zero','Low', 'Medium', 'High']
        categories = pd.cut(feature_data['count'], bins, labels=group_names)
        feature_data['count'] = pd.cut(feature_data['count'], bins, labels=group_names)
        feature_data.rename(columns={'count': self.feature_col}, inplace = True)      

        return feature_data, 'discipline_days_zero_low_medium_high'


class math_3rd_grade_wkce_categorical(AbstractFeature):
    
    def __init__(self, table_name, conn):
        super(math_3rd_grade_wkce_categorical, self).__init__(table_name, conn)    
        
    @property
    def feature_id(self):
        return 10 
    @property
    def feature_col(self):
        return 'math_3rd_grade_wkce_categorical' 
    @property
    def feature_type(self):  
        return 'categorical'

    @property
    def feature_description(self):
        return '''WKCE categorical test results (1:Minimal/ 2:Basic/ 3:Proficient/ 4:Advanced/ M: Missing)
                in the subject of math-- take worst result'''
        
    def feature_code(self):
        
        assess = pd.read_sql_query('''SELECT  student_key, min(test_primary_result_code) as math_3rd_grade_wkce_categorical
         from edu_schema.assessment 
        where test_subject = 'Mathematics' and test_type = 'WKCE'
        and test_primary_result_code in ('1', '2', '3', '4')
        group by student_key 
        ''', self.conn)
        # test_primary_result_code 
        # 1: Minimal 
        # 2: Basic
        # 3: Proficient
        # 4: Advanced
        # 5: @ERR (dropped)

        #get person_id 
        person_id = pd.read_sql_query("SELECT person_id, student_key FROM training.mapping", self.conn)

        #merge on student key
        merged = pd.merge(person_id, assess, on='student_key', how='left')

        #dropping all the other columns except the new feature and person_id 
        feature_data = merged[['person_id','math_3rd_grade_wkce_categorical']]
        
        #pre-processing --- replace nulls with 'M' for missing 
        feature_data = pp.fill_null_with_default_value(feature_data, self.feature_col, 'M')

        return feature_data, "math_3rd_grade_wkce_categorical"
    
#####################################################    
    
class math_3rd_grade_wkce_numerical(AbstractFeature):
    
    def __init__(self, table_name, conn):
        super(math_3rd_grade_wkce_numerical, self).__init__(table_name, conn)    
        
    @property
    def feature_id(self):
        return 11 
    @property
    def feature_col(self):
        return 'math_3rd_grade_wkce_numerical' 
    @property
    def feature_type(self):  
        return 'numerical'
    @property
    def feature_description(self):
        return '''WKCE numerical test results (1:Minimal/ 2:Basic/ 3:Proficient/ 4:Advanced)\
                in the subject of math-- takes the worst one'''
        
    def feature_code(self):
        
        assess = pd.read_sql_query('''SELECT student_key, cast(min(test_primary_result_code) as int) as math_3rd_grade_wkce_numerical 
          from edu_schema.assessment
                where test_subject = 'Mathematics' and test_type = 'WKCE' and test_primary_result_code in ('1', '2', '3', '4')
                group by 1''', self.conn)
        # test_primary_result_code 
        # 1: Minimal 
        # 2: Basic
        # 3: Proficient
        # 4: Advanced
        # 5: @ERR (dropped)
        
        #get person_id 
        person_id = pd.read_sql_query("SELECT person_id, student_key FROM training.mapping", self.conn)

        #merge on student key
        merged = pd.merge(person_id, assess, on='student_key', how='left')

        #dropping all the other columns except the new feature and person_id 
        feature_data = merged[['person_id','math_3rd_grade_wkce_numerical']]

        #renaming feature columns        
        #pre-processing --- replace nulls with mean  
        feature_data = pp.fill_null_with_mean(feature_data, self.feature_col)

        return feature_data, 'math_3rd_grade_wkce_numerical'  
    
#########################################  
    
class math_3rd_grade_map_categorical(AbstractFeature):
    
    def __init__(self, table_name, conn):
        super(math_3rd_grade_map_categorical, self).__init__(table_name, conn)    
        
    @property
    def feature_id(self):
        return 12 
    @property
    def feature_col(self):
        return 'math_3rd_grade_map_categorical' 
    @property
    def feature_type(self):  
        return 'categorical'
    @property
    def feature_description(self):
        return '''MAP SCREENER categorical test results--  takes the worst result
                1:Significantly above target 
                2:On target
                3:Below target
                4:Well below target
                5: Significantly below target
                M:Missing
                in the subject of math'''
    
        
    def feature_code(self):

        assess = pd.read_sql_query('''SELECT student_key, min(test_primary_result_code) as math_3rd_grade_map_categorical from edu_schema.assessment 
        where test_subject = 'Mathematics' and test_type = 'MAP SCREENER' and test_primary_result_code in ('1', '2', '3', '4', '5')
        group by 1''', self.conn)
        # test_primary_result_code 
        # 1: Significantly above target 
        # 2: On target
        # 3: Below target
        # 4: Well below target
        # 5: Significantly below target
        # 6: Untested (dropped)
        
        #get person_id 
        person_id = pd.read_sql_query("SELECT person_id, student_key FROM training.mapping", self.conn)

        #merge on student key
        merged = pd.merge(person_id, assess, on='student_key', how='left')

        #dropping all the other columns except the new feature and person_id 
        feature_data = merged[['person_id','math_3rd_grade_map_categorical']]
        
        #pre-processing --- replace nulls with 'M' for missing 
        feature_data = pp.fill_null_with_default_value(feature_data, self.feature_col, 'M')        

        return feature_data, 'math_3rd_grade_map_categorical'   
        
##########################################

class math_3rd_grade_map_numerical(AbstractFeature):
    
    def __init__(self, table_name, conn):
        super(math_3rd_grade_map_numerical, self).__init__(table_name, conn)    
        
    @property
    def feature_id(self):
        return 13 
    @property
    def feature_col(self):
        return 'math_3rd_grade_map_numerical' 
    @property
    def feature_type(self):  
        return 'numerical'
    @property
    def feature_description(self):
        return '''MAP SCREENER numerical test results = takes the worst result (
                1: Significantly above target
                2: On target
                3: Below target
                4: Well below target
                5: Significantly below target)
                in the subject of math'''
    
        
    def feature_code(self):

        assess = pd.read_sql_query('''SELECT student_key, min(test_primary_result_code) as math_3rd_grade_map_numerical from edu_schema.assessment 
        where test_subject = 'Mathematics' and test_type = 'MAP SCREENER' and test_primary_result_code in ('1', '2', '3', '4', '5')
        group by 1''', self.conn)
        # test_primary_result_code 
        # 1: Significantly above target 
        # 2: On target
        # 3: Below target
        # 4: Well below target
        # 5: Significantly below target
        # 6: Untested (dropped)
      
        assess['math_3rd_grade_map_numerical'] = assess['math_3rd_grade_map_numerical'].apply(int)

        #get person_id 
        person_id = pd.read_sql_query("SELECT person_id, student_key FROM training.mapping", self.conn)

        #merge on student key
        merged = pd.merge(person_id, assess, on='student_key', how='left')

        #dropping all the other columns except the new feature and person_id 
        feature_data = merged[['person_id',self.feature_col]]
        #pre-processing --- replace nulls with mean  
        feature_data = pp.fill_null_with_mean(feature_data, 'math_3rd_grade_map_numerical')

        return feature_data, 'math_3rd_grade_map_numerical' 
    
##########################################    

class math_3rd_grade_ach_categorical(AbstractFeature):
    
    def __init__(self, table_name, conn):
        super(math_3rd_grade_ach_categorical, self).__init__(table_name, conn)    
        
    @property
    def feature_id(self):
        return 14 
    @property
    def feature_col(self):
        return 'math_3rd_grade_ach_categorical' 
    @property
    def feature_type(self):  
        return 'categorical'
    @property
    def feature_description(self):
        return 'Achievement categorical test results (takes the worst) (1:Below Basic/ 2:Basic/ 3:Proficient/ 4:Advanced/ M:Missing) in the subject of math'
        
    def feature_code(self):
        
        assess = pd.read_sql_query('''SELECT  student_key, cast(min(test_primary_result_code) as int) as math_3rd_grade_ach_categorical from edu_schema.assessment 
        where test_subject = 'Math' and test_type = 'Achievement' and test_primary_result_code in ('1', '2', '3', '4')
        group by 1''', self.conn)
        # test_primary_result_code 
        # 1: Below Basic 
        # 2: Basic
        # 3: Proficient
        # 4: Advanced
        # --: Not specified (dropped)
        #get person_id 
        person_id = pd.read_sql_query("SELECT person_id, student_key FROM training.mapping", self.conn)

        #merge on student key
        merged = pd.merge(person_id, assess, on='student_key', how='left')

        #dropping all the other columns except the new feature and person_id 
        feature_data = merged[['person_id','math_3rd_grade_ach_categorical']]
        
        #pre-processing --- replace nulls with 'M' for missing 
        feature_data = pp.fill_null_with_default_value(feature_data, self.feature_col, 'M')

        return feature_data, 'math_3rd_grade_ach_categorical'
    
##############################################   

class math_3rd_grade_ach_numerical(AbstractFeature):
    
    def __init__(self, table_name, conn):
        super(math_3rd_grade_ach_numerical, self).__init__(table_name, conn)    
        
    @property
    def feature_id(self):
        return 15 
    @property
    def feature_col(self):
        return 'math_3rd_grade_ach_numerical' 
    @property
    def feature_type(self):  
        return 'numerical'
    @property
    def feature_description(self):
        return 'Achievement numerical test results (1:Below Basic/ 2:Basic/ 3:Proficient/ 4:Advanced) in the subject of math'
        
    def feature_code(self):
        
        assess = pd.read_sql_query('''SELECT  student_key, cast(min(test_primary_result_code) as int) as  math_3rd_grade_ach_numerical from edu_schema.assessment 
        where test_subject = 'Math' and test_type = 'Achievement' and test_primary_result_code in ('1', '2', '3', '4')
        group by 1''', self.conn)
        # test_primary_result_code 
        # 1: Below Basic 
        # 2: Basic
        # 3: Proficient
        # 4: Advanced
        # --: Not specified (dropped)

        #calculate worst score for students with multiple test scores
        #get person_id 
        person_id = pd.read_sql_query("SELECT person_id, student_key FROM training.mapping", self.conn)

        #merge on student key
        merged = pd.merge(person_id, assess, on='student_key', how='left')

        #dropping all the other columns except the new feature and person_id 
        feature_data = merged[['person_id','math_3rd_grade_ach_numerical']]

        #pre-processing --- replace nulls with mean  
        feature_data = pp.fill_null_with_mean(feature_data, self.feature_col)

        return feature_data, 'math_3rd_grade_ach_numerical'
    
##############################################     
    
class reading_3rd_grade_wkce_categorical(AbstractFeature):
    
    def __init__(self, table_name, conn):
        super(reading_3rd_grade_wkce_categorical, self).__init__(table_name, conn)    
        
    @property
    def feature_id(self):
        return 16 
    @property
    def feature_col(self):
        return 'reading_3rd_grade_wkce_categorical' 
    @property
    def feature_type(self):  
        return 'categorical'
    @property
    def feature_description(self):
        return 'WKCE categorical test results-- takes the worst (1:Minimal/ 2:Basic/ 3:Proficient/ 4:Advanced/ M:Missing) in the subject of reading'
        
    def feature_code(self):
        
        assess = pd.read_sql_query('''SELECT  student_key, min(test_primary_result_code) as reading_3rd_grade_wkce_categorical from edu_schema.assessment
        where test_subject = 'Reading' and test_type = 'WKCE' and test_primary_result_code in ('1', '2', '3', '4')
        group by 1''', self.conn)
        # test_primary_result_code 
        # 1: Minimal 
        # 2: Basic
        # 3: Proficient
        # 4: Advanced
        # 5: @ERR (dropped)
        #get person_id 
        person_id = pd.read_sql_query("SELECT person_id, student_key FROM training.mapping", self.conn)

        #merge on student key
        merged = pd.merge(person_id, assess, on='student_key', how='left')

        #dropping all the other columns except the new feature and person_id 
        feature_data = merged[['person_id','reading_3rd_grade_wkce_categorical']]

        #pre-processing --- replace nulls with 'M' for missing 
        feature_data = pp.fill_null_with_default_value(feature_data, self.feature_col, 'M')

        return feature_data, 'reading_3rd_grade_wkce_categorical'
    
#############################################   

class reading_3rd_grade_wkce_numerical(AbstractFeature):
    
    def __init__(self, table_name, conn):
        super(reading_3rd_grade_wkce_numerical, self).__init__(table_name, conn)    
        
    @property
    def feature_id(self):
        return 17 
    @property
    def feature_col(self):
        return 'reading_3rd_grade_wkce_numerical' 
    @property
    def feature_type(self):  
        return 'numerical'
    @property
    def feature_description(self):
        return 'WKCE numerical test results (1:Minimal/ 2:Basic/ 3:Proficient/ 4:Advanced) in the subject of reading'
        
    def feature_code(self):
        
        assess = pd.read_sql_query('''SELECT  student_key, cast(min(test_primary_result_code) as int) as reading_3rd_grade_wkce_numerical from edu_schema.assessment
        where test_subject = 'Reading' and test_type = 'WKCE' and test_primary_result_code in ('1', '2', '3', '4')
        group by 1''', self.conn)
        # test_primary_result_code 
        # 1: Minimal 
        # 2: Basic
        # 3: Proficient
        # 4: Advanced
        # 5: @ERR (dropped)

        #get person_id 
        person_id = pd.read_sql_query("SELECT person_id, student_key FROM training.mapping", self.conn)

        #merge on student key
        merged = pd.merge(person_id, assess, on='student_key', how='left')

        #dropping all the other columns except the new feature and person_id 
        feature_data = merged[['person_id','reading_3rd_grade_wkce_numerical']]
        
        #pre-processing --- replace nulls with mean  
        feature_data = pp.fill_null_with_mean(feature_data, self.feature_col)

        return feature_data, 'reading_3rd_grade_wkce_numerical'
    
#######################################################################    
    
class reading_3rd_grade_map_categorical(AbstractFeature):
    
    def __init__(self, table_name, conn):
        super(reading_3rd_grade_map_categorical, self).__init__(table_name, conn)    
        
    @property
    def feature_id(self):
        return 18 
    @property
    def feature_col(self):
        return 'reading_3rd_grade_map_categorical' 
    @property
    def feature_type(self):  
        return 'categorical'
    @property
    def feature_description(self):
        return '''MAP SCREENER categorical test results (takes the worst) (1:Significantly above target/ 2:On target 
                3:Below target/ 4:Well below target/ 5: Significantly below target/ M:Missing) in the subject of reading'''
    
        
    def feature_code(self):

        assess = pd.read_sql_query('''SELECT  student_key, max(test_primary_result_code) as  reading_3rd_grade_map_categorical from edu_schema.assessment
        where test_subject = 'Reading' and test_type = 'MAP SCREENER' and test_primary_result_code in ('1', '2', '3', '4', '5')
        group by 1''', self.conn)
        # test_primary_result_code 
        # 1: Significantly above target 
        # 2: On target
        # 3: Below target
        # 4: Well below target
        # 5: Significantly below target
        # 6: Untested (dropped)

        #get person_id 
        person_id = pd.read_sql_query("SELECT person_id, student_key FROM training.mapping", self.conn)

        #merge on student key
        merged = pd.merge(person_id, assess, on='student_key', how='left')

        #dropping all the other columns except the new feature and person_id 
        feature_data = merged[['person_id','reading_3rd_grade_map_categorical']]

        #pre-processing --- replace nulls with 'M' for missing 
        feature_data = pp.fill_null_with_default_value(feature_data, self.feature_col, 'M')

        return feature_data, 'reading_3rd_grade_map_categorical'   
    
########################################  

class reading_3rd_grade_map_numerical(AbstractFeature):
    
    def __init__(self, table_name, conn):
        super(reading_3rd_grade_map_numerical, self).__init__(table_name, conn)    
        
    @property
    def feature_id(self):
        return 19 
    @property
    def feature_col(self):
        return 'reading_3rd_grade_map_numerical' 
    @property
    def feature_type(self):  
        return 'numerical'
    @property
    def feature_description(self):
        return '''MAP SCREENER numerical test results (1:Significantly above target/ 2:On target
                3:Below target/ 4:Well below target/ 5: Significantly below target)
                in the subject of reading'''
    
        
    def feature_code(self):

        assess = pd.read_sql_query('''SELECT student_key, cast( max(test_primary_result_code) as int) as reading_3rd_grade_map_numerical from edu_schema.assessment 
        where test_subject = 'Reading' and test_type = 'MAP SCREENER' and test_primary_result_code in ('1', '2', '3', '4', '5')
        group by 1''', self.conn)
        # test_primary_result_code 
        # 1: Significantly above target 
        # 2: On target
        # 3: Below target
        # 4: Well below target
        # 5: Significantly below target
        # 6: Untested (dropped)

        #get person_id 
        person_id = pd.read_sql_query("SELECT person_id, student_key FROM training.mapping", self.conn)

        #merge on student key
        merged = pd.merge(person_id, assess, on='student_key', how='left')

        #dropping all the other columns except the new feature and person_id 
        feature_data = merged[['person_id','reading_3rd_grade_map_numerical']]
        
        #pre-processing --- replace nulls with mean  
        feature_data = pp.fill_null_with_mean(feature_data, self.feature_col)

        return feature_data, 'reading_3rd_grade_map_numerical'
    
#########################################

class ela_3rd_grade_ach_categorical(AbstractFeature):
    
    def __init__(self, table_name, conn):
        super(ela_3rd_grade_ach_categorical, self).__init__(table_name, conn)    
        
    @property
    def feature_id(self):
        return 20 
    @property
    def feature_col(self):
        return 'ela_3rd_grade_ach_categorical' 
    @property
    def feature_type(self):  
        return 'categorical'
    @property
    def feature_description(self):
        return 'Achievement categorical test results (takes the worst) (1:Below Basic/ 2:Basic/ 3:Proficient/ 4:Advanced/ M:Missing) in the subject of ELA'
        
    def feature_code(self):
        
        assess = pd.read_sql_query('''SELECT student_key, min(test_primary_result_code) as  ela_3rd_grade_ach_categorical 
          from edu_schema.assessment where test_subject = 'ELA'
                                     and test_type = 'Achievement' and test_primary_result_code in ('1', '2', '3', '4')
                                     group by 1''', self.conn)
        # test_primary_result_code 
        # 1: Below Basic 
        # 2: Basic
        # 3: Proficient
        # 4: Advanced
        # --: Not specified (dropped)
        #get person_id 
        person_id = pd.read_sql_query("SELECT person_id, student_key FROM training.mapping", self.conn)

        #merge on student key
        merged = pd.merge(person_id, assess, on='student_key', how='left')

        #dropping all the other columns except the new feature and person_id 
        feature_data = merged[['person_id','ela_3rd_grade_ach_categorical']]

        #pre-processing --- replace nulls with 'M' for missing 
        feature_data = pp.fill_null_with_default_value(feature_data, self.feature_col, 'M')

        return feature_data, 'ela_3rd_grade_ach_categorical' 
    
###############################################

class ela_3rd_grade_ach_numerical(AbstractFeature):
    
    def __init__(self, table_name, conn):
        super(ela_3rd_grade_ach_numerical, self).__init__(table_name, conn)    
        
    @property
    def feature_id(self):
        return 21 
    @property
    def feature_col(self):
        return 'ela_3rd_grade_ach_numerical' 
    @property
    def feature_type(self):  
        return 'numerical'
    @property
    def feature_description(self):
        return 'Achievement numerical test results (takes the worst) (1:Below Basic/ 2:Basic/ 3:Proficient/ 4:Advanced) in the subject of ELA'
        
    def feature_code(self):
        
        assess = pd.read_sql_query('''SELECT student_key, cast(min(test_primary_result_code) as int) as ela_3rd_grade_ach_numerical from edu_schema.assessment 
        where test_subject = 'ELA' and test_type = 'Achievement' and test_primary_result_code in ('1', '2', '3', '4')
        group by 1''', self.conn)
        # test_primary_result_code 
        # 1: Below Basic 
        # 2: Basic
        # 3: Proficient
        # 4: Advanced
        # --: Not specified (dropped)

        #get person_id 
        person_id = pd.read_sql_query("SELECT person_id, student_key FROM training.mapping", self.conn)

        #merge on student key
        merged = pd.merge(person_id, assess, on='student_key', how='left')

        #dropping all the other columns except the new feature and person_id 
        feature_data = merged[['person_id','ela_3rd_grade_ach_numerical']]
        
        #pre-processing --- replace nulls with mean  
        feature_data = pp.fill_null_with_mean(feature_data, self.feature_col)

        return feature_data, 'ela_3rd_grade_ach_numerical'
    
###############################################

class total_num_of_incidents(AbstractFeature):
    
    def __init__(self, table_name, conn):
        super(total_num_of_incidents, self).__init__(table_name, conn)    
        
    @property
    def feature_id(self):
        return 22 
    @property
    def feature_col(self):
        return 'total_num_of_incidents' 
    @property
    def feature_type(self):  
        return 'numerical'
    @property
    def feature_description(self):
        return 'Total number of discipline incidents'
        
    def feature_code(self):
        
        #get discipline
        disc = pd.read_sql_query("SELECT student_key, discipline_start_date FROM edu_schema.discipline", self.conn)
        #extract discipline year 
        disc['discipline_year'] = disc['discipline_start_date'].astype('datetime64[ns]').dt.year
        disc = disc[['student_key','discipline_year']]

        #get person_id 
        ids = pd.read_sql_query("SELECT person_id ,student_key FROM training.mapping", self.conn)
        
        #get mode and threshold 
        rf_type, timeframe = self.extract_timeframe_from_table_name() 

        if rf_type == 'year':
            #keep only the pre-timeframe data
            disc_year = disc[disc['discipline_year'] <= timeframe]
            #group by student_key 
            d = disc_year.groupby(disc_year['student_key'])['discipline_year'].count()
            df = pd.DataFrame({'student_key':d.index, 'count':d.values})
            
        elif rf_type == 'age':
            #get demo 
            demo = pd.read_sql_query("SELECT student_key, student_birthdate FROM edu_schema.demographic", self.conn)
            #get birth_year
            demo['birth_year'] = demo['student_birthdate'].astype('datetime64[ns]').dt.year
            demo = demo[['student_key','birth_year']]
            #merge with discipline on student_key
            df = pd.merge(demo, disc, on = 'student_key')
            #calculate age at discipline
            df['age_at_discipline'] = df['discipline_year'] - df['birth_year'] 
            #keep only the pre-timeframe data
            df = df[df['age_at_discipline'] <= timeframe]
            df = df[['student_key','age_at_discipline']]
            #group by student_key 
            d = df.groupby(df['student_key'])['age_at_discipline'].count()
            df = pd.DataFrame({'student_key':d.index, 'count':d.values})

        #merge with ids on student_key
        feature_data = pd.merge(ids, df, on = 'student_key', how = 'left')
        feature_data = feature_data[['person_id','count']]
        feature_data.rename(columns={'count': self.feature_col}, inplace = True)
        #replace nulls with 0
        feature_data = pp.fill_null_with_default_value(feature_data, self.feature_col, 0)
       
        return feature_data, "total_num_of_incidents"
        
#############################################

class num_of_incidents_learning_environment(AbstractFeature):
    
    def __init__(self, table_name, conn):
        super(num_of_incidents_learning_environment, self).__init__(table_name, conn)    
        
    @property
    def feature_id(self):
        return 23 
    @property
    def feature_col(self):
        return 'num_of_incidents_learning_environment' 
    @property
    def feature_type(self):  
        return 'numerical'
    @property
    def feature_description(self):
        return 'Total number of discipline incidents categorized as learning environment'
        
    def feature_code(self):
        
        #get discipline
        disc = pd.read_sql_query("SELECT student_key, discipline_start_date FROM edu_schema.discipline where discipline_fed_offense_group = 'Learning Environment'", self.conn)
        #extract discipline year 
        disc['discipline_year'] = disc['discipline_start_date'].astype('datetime64[ns]').dt.year
        disc = disc[['student_key','discipline_year']]

        #get person_id 
        ids = pd.read_sql_query("SELECT person_id ,student_key FROM training.mapping", self.conn)
        
        #get mode and threshold 
        rf_type, timeframe = self.extract_timeframe_from_table_name() 

        if rf_type == 'year':
            #keep only the pre-timeframe data
            disc_year = disc[disc['discipline_year'] <= timeframe]
            #group by student_key 
            d = disc_year.groupby(disc_year['student_key'])['discipline_year'].count()
            df = pd.DataFrame({'student_key':d.index, 'count':d.values})
            
        elif rf_type == 'age':
            #get demo 
            demo = pd.read_sql_query("SELECT student_key, student_birthdate FROM edu_schema.demographic", self.conn)
            #get birth_year
            demo['birth_year'] = demo['student_birthdate'].astype('datetime64[ns]').dt.year
            demo = demo[['student_key','birth_year']]
            #merge with discipline on student_key
            df = pd.merge(demo, disc, on = 'student_key')
            #calculate age at discipline
            df['age_at_discipline'] = df['discipline_year'] - df['birth_year'] 
            #keep only the pre-timeframe data
            df = df[df['age_at_discipline'] <= timeframe]
            df = df[['student_key','age_at_discipline']]
            #group by student_key 
            d = df.groupby(df['student_key'])['age_at_discipline'].count()
            df = pd.DataFrame({'student_key':d.index, 'count':d.values})
            
        #merge with ids on student_key
        feature_data = pd.merge(ids, df, on = 'student_key', how = 'left')
        feature_data = feature_data[['person_id','count']]
        feature_data.rename(columns={'count': self.feature_col}, inplace = True)
        #replace nulls with 0
        feature_data = pp.fill_null_with_default_value(feature_data, self.feature_col, 0)
       
        return feature_data, 'num_of_incidents_learning_environment'
        
############################################################



class student_504_indicator(AbstractFeature):
    def __init__(self, table_name, conn):
        super(student_504_indicator, self).__init__(table_name, conn)    
        
    @property
    def feature_id(self):
        return 13

    @property
    def feature_col(self):
        return 'student_504_indicator' 

    @property
    def feature_type(self):  
        return 'boolean'

    @property
    def feature_description(self):
        return '''students' participation in 504 program'''
        
    def feature_code(self):
        ''' function should get the features table name, the current feature name and 
        calculate the feature and insert it to the table 
        '''
        rf_type, timeframe = self.extract_timeframe_from_table_name() 
        if rf_type == 'year': 
            sql = '''SELECT person_id, case when student_504_indicator = 'No' then 0 else 1 end as student_504_indicator  from (
                      select student_key, student_504_indicator from 
                         (select student_key, student_504_indicator,
                          row_number() OVER (PARTITION BY student_key ORDER BY year DESC) as rk
                            from edu_schema.demographic 
                               where year <= {}) as t
                                     where rk=1) as f
                                right join training.mapping b on f.student_key = b.student_key'''.format(timeframe)
            
        data = pd.read_sql_query(sql, self.conn)
        data = pp.fill_null_with_zero(data, self.feature_col) 
        return data, "student_504_indicator"

class student_indian_ed_indicator(AbstractFeature):
    def __init__(self, table_name, conn):
        super(student_indian_ed_indicator, self).__init__(table_name, conn)    
        
    @property
    def feature_id(self):
        return 138

    @property
    def feature_col(self):
        return 'student_indian_ed_indicator' 

    @property
    def feature_type(self):  
        return 'boolean'

    @property
    def feature_description(self):
        return '''students' participation in student_indian_ed_indicator'''
        
    def feature_code(self):
        ''' function should get the features table name, the current feature name and 
        calculate the feature and insert it to the table 
        '''
        rf_type, timeframe = self.extract_timeframe_from_table_name() 
        if rf_type == 'year': 
            sql = '''SELECT person_id, case when student_indian_ed_indicator = 'No' then 0 else 1 end as student_indian_ed_indicator from (
                      select student_key, student_indian_ed_indicator from 
                         (select student_key, student_indian_ed_indicator,
                          row_number() OVER (PARTITION BY student_key ORDER BY year DESC) as rk
                            from edu_schema.demographic 
                               where year <= {}) as t
                                     where rk=1) as f
                                right join training.mapping b on f.student_key = b.student_key'''.format(timeframe)
            
        data = pd.read_sql_query(sql, self.conn)
        data = pp.fill_null_with_zero(data, self.feature_col) 
        return data, "student_indian_ed_indicator"




class student_migrant_ed_indicator(AbstractFeature):
    def __init__(self, table_name, conn):
        super(student_migrant_ed_indicator, self).__init__(table_name, conn)    
        
    @property
    def feature_id(self):
        return 138

    @property
    def feature_col(self):
        return 'student_migrant_ed_indicator' 

    @property
    def feature_type(self):  
        return 'boolean'

    @property
    def feature_description(self):
        return '''students' participation in student_migrant_ed_indicator'''
        
    def feature_code(self):
        ''' function should get the features table name, the current feature name and 
        calculate the feature and insert it to the table 
        '''
        rf_type, timeframe = self.extract_timeframe_from_table_name() 
        if rf_type == 'year': 
            sql = '''SELECT person_id, case when student_migrant_ed_indicator = 'No' then 0 else 1 end as student_migrant_ed_indicator from (
                      select * from 
                         (select student_key, student_migrant_ed_indicator,
                          row_number() OVER (PARTITION BY student_key ORDER BY year DESC) as rk
                            from edu_schema.demographic 
                               where year <= {}) as t
                                     where rk=1) as f
                                right join training.mapping b on f.student_key = b.student_key'''.format(timeframe)
            
        data = pd.read_sql_query(sql, self.conn)
        data = pp.fill_null_with_zero(data, self.feature_col)
        return data, "student_migrant_ed_indicator"


class AbstractProgramFeature(AbstractFeature):
    
    @property
    def feature_col(self):
        return self.__class__.__name__
    
    @property
    def feature_type(self):
        return 'boolean'
    
    @abc.abstractproperty
    def program_name(self):
        pass
    
    def feature_code(self):
        rf_type, timeframe = self.extract_timeframe_from_table_name() 
        if rf_type == 'year':
            sql = '''SELECT distinct(person_id) as pid 
            from edu_schema.programs a 
            left join
            (select distinct(student_id), student_key 
            from edu_schema.new_demographic) b on a.student_id = b.student_id 
            right join training.mapping c 
            on b.student_key = c.student_key  where 
            program_name = %(program_name)s
            and extract(year from begin_date) <= %(timeframe)s'''
            
        else:
            sql = ''' select distinct(person_id) as pid 
            from edu_schema.programs a left join 
            (select distinct(student_id), student_key from edu_schema.new_demographic) b 
            on a.student_id = b.student_id right join training.mapping c  
            on b.student_key = c.student_key  where program_name = %(program_name)s
            and extract(year from begin_date) - 
            extract(year from student_birthdate) <= %(timeframe)s'''          
        data = pd.read_sql_query(sql, self.conn, params={'timeframe': timeframe, 'program_name': self.program_name })
        allid = pd.read_sql_query("select person_id from training.mapping", self.conn)
        allid[self.feature_col] = 0
        allid = pd.merge(allid, data, left_on = 'person_id', right_on = 'pid', how = 'left')
        allid.ix[allid['pid'].notnull(), self.feature_col] = 1
        feature_data = allid[['person_id', self.feature_col]]
        
        return feature_data, self.feature_col

    
class is_student_in_tabs(AbstractProgramFeature):
          
    @property
    def feature_id(self):
        return 101

    @property
    def feature_description(self):
        return '''return 1 if student is enrolled in TABS (Truancy Abatement and 
        Burglary Suppression) , 0 otherwise'''
    
    @property
    def program_name(self):
        return 'TABS'
            
    
class is_student_homeless(AbstractProgramFeature):
    
    @property
    def feature_id(self):
        return 102
    
    @property
    def feature_description(self):
        return '''return a dataframe of 1 if student is homeless, 0 otherwise'''
    
    @property 
    def program_name(self):
        return 'Homeless'

    
class is_student_atrisk(AbstractProgramFeature):
    
    @property
    def feature_id(self):
        return 103
    
    @property
    def feature_description(self):
        return '''return a dataframe of 1 if student is at risk, 0 otherwise'''
    
    @property
    def program_name(self):
        return 'At Risk'
         
    
class is_student_bilingual(AbstractProgramFeature):

    @property
    def feature_id(self):
        return 104
    
    @property
    def feature_description(self):
        return '''return a dataframe of 1 if student is on "bilingual program", 0 otherwise'''
    
    @property    
    def program_name(self):
        return 'Bilingual Program' 
            

class is_mckinney_vento(AbstractProgramFeature):
    
    @property
    def feature_id(self):
        return 105 
    
    @property
    def feature_description(self):
        return '''return a dataframe of 1 if student is on "mckinney_vento", 0 otherwise'''
    
    @property
    def program_name(self):
        return 'McKinney Vento'
       
    
class is_esl(AbstractProgramFeature):
     
    @property
    def feature_id(self):
        return 106
    
    @property
    def feature_description(self):
        return '''return a dataframe of 1 if student is on "esl program", 0 otherwise'''
    
    @property
    def program_name(self):
        return 'ESL Program' 
        
         
class is_headstart(AbstractProgramFeature):
    
    @property
    def feature_id(self):
        return 107
    
    @property
    def feature_description(self):
        return '''return a dataframe of 1 if student is on "headstart", 0 otherwise'''
    
    @property    
    def program_name(self):
        return 'Head Start' 
 
    
class is_intradistrict(AbstractProgramFeature):
    
    @property
    def feature_id(self):
        return 108
    
    @property
    def feature_description(self):
         return '''return a dataframe of 1 if student is on "Intra District", 0 otherwise'''
    
    @property
    def program_name(self):
        return 'Intra District'
    
    
class is_teacher_support(AbstractProgramFeature):
    
    @property
    def feature_id(self):
        return 109
    
    @property
    def feature_description(self):
        return '''return a dataframe of 1 if student is on "Intra District", 0 otherwise'''
    
    @property
    def program_name(self):
        return 'Teacher Instruction with Support'
    
    
class is_student_sp_ed(AbstractProgramFeature):
          
    @property
    def feature_id(self):
        return 110

    @property
    def feature_description(self):
        return '''return a dataframe of 1 if student is on "SPED Referral", 0 otherwise'''
        
    @property
    def program_name(self):
        return 'SPED Referral'
    

class is_schoolage_parent(AbstractProgramFeature):
          
    @property
    def feature_id(self):
        return 110

    @property
    def feature_description(self):
        return '''return a dataframe of 1 if student is on "School Age Parent", 0 otherwise'''
        
    @property
    def program_name(self):
        return 'School Age Parent'
    
    
class is_city_transport(AbstractProgramFeature):
          
    @property
    def feature_id(self):
        return 110

    @property
    def feature_description(self):
        return '''return a dataframe of 1 if student is on "Citywide Transportation", 0 otherwise'''
        
    @property
    def program_name(self):
        return 'Citywide Transportation'
    
#######################################################


class student_race(AbstractFeature):
    
    def __init__(self, table_name, conn):
        super(student_race, self).__init__(table_name, conn)    
        
    @property
    def feature_id(self):
        return 300 
    @property
    def feature_col(self):
        return 'student_race' 
    @property
    def feature_type(self):  
        return 'categorical'
    @property
    def feature_description(self):
        return '''check if the student should exist in this table, based on the given year'''
        
    def feature_code(self):

        sql = '''select distinct(person_id), student_race from edu_schema.demographic a 
        right join training.mapping b on a.student_key = b.student_key'''

        data = pd.read_sql_query(sql, self.conn)
        return data, "student_race"

    
class age(AbstractFeature):
    
    def __init__(self, table_name, conn):
        super(age, self).__init__(table_name, conn)    
        
    @property
    def feature_id(self):
        return 301
    @property
    def feature_col(self):
        return self.__class__.__name__
    @property
    def feature_type(self):  
        return 'numerical'
    @property
    def feature_description(self):
        return '''check if the student should exist in this table, based on the given year.
                  Note that we throw out students who are less than three years old
                  because that's probably a mistake (or, at least not relevant to our task).'''

    def feature_code(self):

        rf_type, timeframe = self.extract_timeframe_from_table_name() 
        if rf_type == 'year': 
            sql = '''SELECT DISTINCT person_id, ({timeframe} - extract(year from student_birthdate)) as {feature_col}
                     from edu_schema.demographic a 
                     right join training.mapping b on a.student_key = b.student_key
                     '''.format(timeframe=timeframe, feature_col=self.feature_col)
        else: 
            raise ValueError("No need for age feature in age model.")
        data = pd.read_sql_query(sql, self.conn)
        age_mean = data.loc[data[self.feature_col] <= 19, self.feature_col].mean()
        data.loc[data[self.feature_col] < 3, self.feature_col] = int(age_mean)
        data = pp.fill_null_with_default_value(data, self.feature_col, age_mean)
        return data, self.feature_col


class age_categorical(age):
    @property
    def feature_type(self):
        return 'categorical'


class student_gender(AbstractFeature):
    
    def __init__(self, table_name, conn):
        super(student_gender, self).__init__(table_name, conn)    
        
    @property
    def feature_id(self):
        return 3001
    @property
    def feature_col(self):
        return 'student_gender' 
    @property
    def feature_type(self):  
        return 'categorical'

    @property
    def feature_description(self):
        return '''return a dataframe of person_id and gender'''
        
    def feature_code(self):
        
        sql = ''' SELECT distinct(person_id), student_gender from edu_schema.new_demographic a 
        right join training.mapping b on a.student_key = b.student_key'''
        
        feature_data = pd.read_sql_query(sql, self.conn)
        feature_data = pp.fill_null_with_zero(feature_data, 'student_gender')
        
        return feature_data, 'student_gender'


    
########################### Enrollment

class enroll_records_per_year(AbstractFeature):
    def __init__(self, table_name, conn):
        super(enroll_records_per_year, self).__init__(table_name, conn)    
        
    @property
    def feature_id(self):
        return 400

    @property
    def feature_col(self):
        return 'enroll_records_per_year' 

    @property
    def feature_type(self):  
        return 'numerical'

    @property
    def feature_description(self):
        return '''students' average number of enrollment records per year'''
        
    def feature_code(self):

        rf_type, timeframe = self.extract_timeframe_from_table_name() 
        if rf_type == 'year': 
            sql = '''  SELECT person_id, 
                        CASE WHEN num_years > 0 then cast(num_records as float) / num_years ELSE 0 end as enroll_records_per_year 
                             from (SELECT 
                                   a.student_key,
                                      b.person_id, 
                                         count(*) as num_records, 
                                           count(distinct school_year) as num_years
                             FROM edu_schema.enrollment a
                             right JOIN training.mapping b ON a.student_key = b.student_key
                             AND year <= {}
                             group by 1,2) as t'''.format(timeframe)
            
        data = pd.read_sql_query(sql, self.conn)
        return data, "enroll_records_per_year"



class enroll_days_last_year(AbstractFeature):
    def __init__(self, table_name, conn):
        super(enroll_days_last_year, self).__init__(table_name, conn)    
        
    @property
    def feature_id(self):
        return 22

    @property
    def feature_col(self):
        return 'enroll_days_last_year' 

    @property
    def feature_type(self):  
        return 'numerical'

    @property
    def feature_description(self):
        return '''students' number of enrollment days on last year '''
        
    def feature_code(self):
        ''' function should get the features table name, the current feature name and 
        calculate the feature and insert it to the table 
        '''
        rf_type, timeframe = self.extract_timeframe_from_table_name() 
        if rf_type == 'year': 
            sql = '''SELECT person_id, cast(min(enrollment_days) as float) as enroll_days_last_year
                             from edu_schema.enrollment a
                                right JOIN training.mapping b ON a.student_key = b.student_key
                                   AND a.year = {}
                                   group by 1'''.format(timeframe)
        data = pd.read_sql_query(sql, self.conn)
        data = pp.fill_null_with_mean(data, "enroll_days_last_year")
        return data, "enroll_days_last_year"




class end_enroll_not_in_time(AbstractFeature):
    def __init__(self, table_name, conn):
        super(end_enroll_not_in_time, self).__init__(table_name, conn)    
        
    @property
    def feature_id(self):
        return 232

    @property
    def feature_col(self):
        return 'end_enroll_not_in_time' 

    @property
    def feature_type(self):  
        return 'boolean'

    @property
    def feature_description(self):
        return '''students' end enrollment date was not in time (later or earlier then June)'''
        
    def feature_code(self):
        ''' function should get the features table name, the current feature name and 
        calculate the feature and insert it to the table 
        '''
        rf_type, timeframe = self.extract_timeframe_from_table_name() 
        sql = '''SELECT person_id, cast(max(enroll_month) as float) as end_enroll_not_in_time 
                    from (select student_key,  case when extract(month from end_cd) = 6 then 0 else 1 end as enroll_month
                      from edu_schema.enrollment
                        where extract(year from end_cd)<= {}) as t
                            right join training.mapping b on t.student_key = b.student_key
                            group by 1'''.format(timeframe)
        data = pd.read_sql_query(sql, self.conn)
        data = pp.fill_null_with_zero(data, "end_enroll_not_in_time")
        return data, "end_enroll_not_in_time"



class adm_enroll_not_in_time(AbstractFeature):
    def __init__(self, table_name, conn):
        super(adm_enroll_not_in_time, self).__init__(table_name, conn)    
        
    @property
    def feature_id(self):
        return 233

    @property
    def feature_col(self):
        return 'adm_enroll_not_in_time' 

    @property
    def feature_type(self):  
        return 'boolean'

    @property
    def feature_description(self):
        return '''students' admission enrollment date was not in time (later or earlier then September)'''
        
    def feature_code(self):
        ''' function should get the features table name, the current feature name and 
        calculate the feature and insert it to the table 
        '''
        rf_type, timeframe = self.extract_timeframe_from_table_name() 
        sql = '''SELECT person_id, max(enroll_month) as adm_enroll_not_in_time 
                    from (select student_key,  case when extract(month from adm_cd) = 9 then 0 else 1 end as enroll_month
                      from edu_schema.enrollment
                        where extract(year from adm_cd)<= {}) as t
                            right join training.mapping b on t.student_key = b.student_key
                            group by 1'''.format(timeframe)
        data = pd.read_sql_query(sql, self.conn)
        data = pp.fill_null_with_zero(data, "adm_enroll_not_in_time")
        return data, "adm_enroll_not_in_time"


class adm_enroll_month_last_year(AbstractFeature):
    def __init__(self, table_name, conn):
        super(adm_enroll_month_last_year, self).__init__(table_name, conn)    
        
    @property
    def feature_id(self):
        return 234

    @property
    def feature_col(self):
        return 'adm_enroll_month_last_year' 

    @property
    def feature_type(self):  
        return 'categorical'

    @property
    def feature_description(self):
        return '''students' admission enrollment date was not in time (later or earlier then September)'''
        
    def feature_code(self):
        ''' function should get the features table name, the current feature name and 
        calculate the feature and insert it to the table 
        '''
        rf_type, timeframe = self.extract_timeframe_from_table_name() 
        sql = '''SELECT distinct on  (person_id) person_id, adm_enroll_month_last_year 
                    from (select student_key,   extract(month from adm_cd) as adm_enroll_month_last_year
                      from edu_schema.enrollment
                        where extract(year from adm_cd)= {}
                        order by adm_cd DESC) as t
                            right join training.mapping b on t.student_key = b.student_key'''.format(timeframe)
        data = pd.read_sql_query(sql, self.conn)
        data = pp.fill_null_with_zero(data, "adm_enroll_month_last_year")
        return data, "adm_enroll_month_last_year"



class end_enroll_month_last_year(AbstractFeature):
    def __init__(self, table_name, conn):
        super(end_enroll_month_last_year, self).__init__(table_name, conn)    
        
    @property
    def feature_id(self):
        return 235

    @property
    def feature_col(self):
        return 'end_enroll_month_last_year' 

    @property
    def feature_type(self):  
        return 'categorical'

    @property
    def feature_description(self):
        return '''students' end enrollment month during last year '''
        
    def feature_code(self):
        ''' function should get the features table name, the current feature name and 
        calculate the feature and insert it to the table 
        '''
        rf_type, timeframe = self.extract_timeframe_from_table_name() 
        sql = '''SELECT  distinct on  (person_id) person_id, end_enroll_month_last_year 
                    from (select student_key,   extract(month from end_cd) as end_enroll_month_last_year
                      from edu_schema.enrollment
                        where extract(year from end_cd)= {}
                        order by end_cd DESC) as t
                            right join training.mapping b on t.student_key = b.student_key'''.format(timeframe)
        data = pd.read_sql_query(sql, self.conn)
        data = pp.fill_null_with_zero(data, "end_enroll_month_last_year")
        return data, "end_enroll_month_last_year"



class school(AbstractFeature):
    
    def __init__(self, table_name, conn):
        super(school, self).__init__(table_name, conn)    
        
    @property
    def feature_id(self):
        return 3001
    @property
    def feature_col(self):
        return 'school' 
    @property
    def feature_type(self):  
        return 'categorical'

    @property
    def feature_description(self):
        return '''return a dataframe of person_id and school'''
        
    def feature_code(self):
        rf_type, timeframe = self.extract_timeframe_from_table_name() 
        sql = ''' SELECT person_id, cast(countable_school_code as text) as school  from (
                      select student_key, countable_school_code from 
                         (select student_key, countable_school_code,
                          row_number() OVER (PARTITION BY student_key ORDER BY collection_date DESC) as rk
                            from edu_schema.new_demographic 
                               where extract(year from collection_date) <= {}) as t
                                     where rk=1) as f
                                right join training.mapping b on f.student_key = b.student_key'''.format(timeframe) 
        
        feature_data = pd.read_sql_query(sql, self.conn)
        feature_data = pp.fill_null_with_default_value(feature_data, 'school', '0000')
        
        return feature_data, 'school'


class schools_per_student(AbstractFeature):
    
    def __init__(self, table_name, conn):
        super(schools_per_student, self).__init__(table_name, conn)    
        
    @property
    def feature_id(self):
        return 3002
    @property
    def feature_col(self):
        return 'schools_per_student' 
    @property
    def feature_type(self):  
        return 'numerical'

    @property
    def feature_description(self):
        return '''return a dataframe of person_id and schools_per_student'''
        
    def feature_code(self):
        rf_type, timeframe = self.extract_timeframe_from_table_name() 
        
        sql = '''SELECT person_id, schools_per_student   from (
                      select student_key, count(distinct countable_school_name) as schools_per_student  
                            from edu_schema.new_demographic 
                               where extract(year from collection_date) <= {}
                               group by 1) as t
                                right join training.mapping b on t.student_key = b.student_key'''.format(timeframe) 
        
        feature_data = pd.read_sql_query(sql, self.conn)
        feature_data = pp.fill_null_with_zero(feature_data, 'schools_per_student')
        
        return feature_data, 'schools_per_student'

class student_city(AbstractFeature):
    
    def __init__(self, table_name, conn):
        super(student_city, self).__init__(table_name, conn)    
        
    @property
    def feature_id(self):
        return 3001
    @property
    def feature_col(self):
        return 'student_city' 
    @property
    def feature_type(self):  
        return 'categorical'

    @property
    def feature_description(self):
        return '''return a dataframe of person_id and student_city'''
        
    def feature_code(self):
        
        rf_type, timeframe = self.extract_timeframe_from_table_name() 

        sql = ''' SELECT person_id, left(lower(student_city),4) as student_city  from (
                      select student_key, student_city from 
                         (select student_key, student_city,
                          row_number() OVER (PARTITION BY student_key ORDER BY collection_date DESC) as rk
                            from edu_schema.new_demographic 
                               where extract(year from collection_date) <= {}) as t
                                     where rk=1) as f
                                right join training.mapping b on f.student_key = b.student_key'''.format(timeframe) 
        
        feature_data = pd.read_sql_query(sql, self.conn)
        feature_data = pp.fill_null_with_default_value(feature_data, 'student_city', 'unknown')
        
        return feature_data, 'student_city'
    
class best_score_reading_map(AbstractAssessmentFeature):
    
    table = 'new_assessment_with_date'
    aggregate_function = 'MIN'
    test_subject = 'Reading'
    test_type = 'MAP SCREENER'
    test_primary_result_code = ('1', '2', '3', '4', '5')
    # 0: Not Applicable (dropped)
    # 1: Significantly above target 
    # 2: On target
    # 3: Below target
    # 4: Well below target
    # 5: Significantly below target
    # 6: Untested (dropped)
    
    default_value = 'M'

    @property
    def feature_id(self):
        return 100 

    @property
    def feature_description(self):
        return 'Best score in MAP SCREENER test, subject: Reading'
    

class worst_score_reading_map(AbstractAssessmentFeature):
    
    table = 'new_assessment_with_date'
    aggregate_function = 'MAX'
    test_subject = 'Reading'
    test_type = 'MAP SCREENER'
    test_primary_result_code = ('1', '2', '3', '4', '5')
    # 0: Not Applicable (dropped)
    # 1: Significantly above target 
    # 2: On target
    # 3: Below target
    # 4: Well below target
    # 5: Significantly below target
    # 6: Untested (dropped)
    
    default_value = 'M'
 

    @property
    def feature_id(self):
        return 101 

    @property
    def feature_description(self):
        return 'Worst score in MAP SCREENER test, subject: Reading'
    
    
class best_score_math_map(AbstractAssessmentFeature):
    
    table = 'new_assessment_with_date'
    aggregate_function = 'MIN'
    test_subject = 'Mathematics'
    test_type = 'MAP SCREENER'
    test_primary_result_code = ('1', '2', '3', '4', '5')
    # 0: Not Applicable (dropped)
    # 1: Significantly above target 
    # 2: On target
    # 3: Below target
    # 4: Well below target
    # 5: Significantly below target
    # 6: Untested (dropped)
    
    default_value = 'M'

    @property
    def feature_id(self):
        return 102 

    @property
    def feature_description(self):
        return 'Best score in MAP SCREENER test, subject: Mathematics'  
    
class worst_score_math_map(AbstractAssessmentFeature):
    
    table = 'new_assessment_with_date'
    aggregate_function = 'MAX'
    test_subject = 'Mathematics'
    test_type = 'MAP SCREENER'
    test_primary_result_code = ('1', '2', '3', '4', '5')
    # 0: Not Applicable (dropped)
    # 1: Significantly above target 
    # 2: On target
    # 3: Below target
    # 4: Well below target
    # 5: Significantly below target
    # 6: Untested (dropped)
    
    default_value = 'M'

    @property
    def feature_id(self):
        return 103 

    @property
    def feature_description(self):
        return 'Worst score in MAP SCREENER test, subject: Mathematics'
    
#######################################################################   

class reading_map_slope(AbstractAssessmentSlope):   
    
    table = 'new_assessment_with_date'
    test_subject = 'Reading'
    test_type = 'MAP SCREENER'
    test_primary_result_code = ('1', '2', '3', '4', '5')
    # 0: Not Applicable (dropped)
    # 1: Significantly above target 
    # 2: On target
    # 3: Below target
    # 4: Well below target
    # 5: Significantly below target
    # 6: Untested (dropped)
   
    bins = [-float('Inf'), -1.99, 1.99, float('Inf')]
    group_names = ['neg', 'zero', 'pos']
    # changes of socre level: 
    # 0, 1, -1 --> zero  
    # -4, -3, -2 --> neg
    # 4, 3, 2 --> pos
    
    default_value = 'zero'

    @property
    def feature_id(self):
        return 104 

    @property
    def feature_description(self):
        return 'Change in score in MAP SCREENER test, subject: Reading'
    
    
class math_map_slope(AbstractAssessmentSlope):   
    
    table = 'new_assessment_with_date'
    test_subject = 'Mathematics'
    test_type = 'MAP SCREENER'
    test_primary_result_code = ('1', '2', '3', '4', '5')
    # 0: Not Applicable (dropped)
    # 1: Significantly above target 
    # 2: On target
    # 3: Below target
    # 4: Well below target
    # 5: Significantly below target
    # 6: Untested (dropped)
   
    bins = [-float('Inf'), -1.99, 1.99, float('Inf')]
    group_names = ['neg', 'zero', 'pos']
    # changes of socre level: 
    # 0, 1, -1 --> zero  
    # -4, -3, -2 --> neg
    # 4, 3, 2 --> pos
    
    default_value = 'zero'

    @property
    def feature_id(self):
        return 105 

    @property
    def feature_description(self):
        return 'Change in score in MAP SCREENER test, subject: Mathematics'    
        

class avg_discipline_per_year(AbstractDisciplineFeature):   
    
    table = 'discipline_with_year'
    offense_group = ('Learning Environment', 'Weapons', 'Personal/Physical Safety')
    aggregate_function = 'AVG'
    default_value = 0
    mode = 'forever'
   

    @property
    def feature_id(self):
        return 164

    @property
    def feature_description(self):
        return 'Average discipline days per year'     
    
    
class avg_discipline_weapons_per_year(AbstractDisciplineFeature):   
    
    table = 'discipline_with_year'
    offense_group = ('Weapons', 'foo')
    aggregate_function = 'AVG'
    default_value = 0
    mode = 'forever'

    @property
    def feature_id(self):
        return 145 

    @property
    def feature_description(self):
        return 'Average weapons-related discipline days per year'       
    

class avg_discipline_physical_per_year(AbstractDisciplineFeature):   
    
    table = 'discipline_with_year'
    offense_group = ('Personal/Physical Safety', 'foo')
    aggregate_function = 'AVG'
    default_value = 0
    mode = 'forever'

    @property
    def feature_id(self):
        return 145 

    @property
    def feature_description(self):
        return 'Average physical safety-related discipline days per year'
    
    
class avg_discipline_learning_per_year(AbstractDisciplineFeature):   
    
    table = 'discipline_with_year'
    offense_group = ('Learning Environment', 'foo')
    aggregate_function = 'AVG'
    default_value = 0
    mode = 'forever'

    @property
    def feature_id(self):
        return 67 

    @property
    def feature_description(self):
        return 'Average learning environment-related discipline days per year'     
    
    
class max_discipline_per_year(AbstractDisciplineFeature):   
    
    table = 'discipline_with_year'
    offense_group = ('Learning Environment', 'Weapons', 'Personal/Physical Safety')
    aggregate_function = 'MAX'
    default_value = 0
    mode = 'forever'

    @property
    def feature_id(self):
        return 164

    @property
    def feature_description(self):
        return 'Maximum number of discipline days over year'     
    

class max_discipline_weapons_per_year(AbstractDisciplineFeature):   
    
    table = 'discipline_with_year'
    offense_group = ('Weapons', 'foo')
    aggregate_function = 'MAX'
    default_value = 0
    mode = 'forever'

    @property
    def feature_id(self):
        return 145 

    @property
    def feature_description(self):
        return 'Maximum number of weapons-related discipline days over year'       
    

class max_discipline_physical_per_year(AbstractDisciplineFeature):   
    
    table = 'discipline_with_year'
    offense_group = ('Personal/Physical Safety', 'foo')
    aggregate_function = 'MAX'
    default_value = 0
    mode = 'forever'

    @property
    def feature_id(self):
        return 145 

    @property
    def feature_description(self):
        return 'Maximum number of physical safety-related discipline days over year'
    
    
class max_discipline_learning_per_year(AbstractDisciplineFeature):   
    
    table = 'discipline_with_year'
    offense_group = ('Learning Environment', 'foo')
    aggregate_function = 'MAX'
    default_value = 0
    mode = 'forever'

    @property
    def feature_id(self):
        return 67 

    @property
    def feature_description(self):
        return 'Maximum number of learning environment-related discipline days over year' 
    

class std_discipline_per_year(AbstractDisciplineFeature):   
    
    table = 'discipline_with_year'
    offense_group = ('Learning Environment', 'Weapons', 'Personal/Physical Safety')
    aggregate_function = 'STDDEV'
    default_value = 0
    mode = 'forever'
   

    @property
    def feature_id(self):
        return 164

    @property
    def feature_description(self):
        return 'Standard deviation of discipline days over the years'     
    
    
class std_discipline_weapons_per_year(AbstractDisciplineFeature):   
    
    table = 'discipline_with_year'
    offense_group = ('Weapons', 'foo')
    aggregate_function = 'STDDEV'
    default_value = 0
    mode = 'forever'

    @property
    def feature_id(self):
        return 145 

    @property
    def feature_description(self):
        return 'Standard deviation of weapons-related discipline days over the years'       
    

class std_discipline_physical_per_year(AbstractDisciplineFeature):   
    
    table = 'discipline_with_year'
    offense_group = ('Personal/Physical Safety', 'foo')
    aggregate_function = 'STDDEV'
    default_value = 0
    mode = 'forever'

    @property
    def feature_id(self):
        return 145 

    @property
    def feature_description(self):
        return 'Standard deviation of physical safety-related discipline days over the years'
    
    
class std_discipline_learning_per_year(AbstractDisciplineFeature):   
    
    table = 'discipline_with_year'
    offense_group = ('Learning Environment', 'foo')
    aggregate_function = 'STDDEV'
    default_value = 0
    mode = 'forever'

    @property
    def feature_id(self):
        return 67 

    @property
    def feature_description(self):
        return 'Standard deviation of learning environment-related discipline days over the years' 
    

class sum_discipline_last_year(AbstractDisciplineFeature):   
    
    table = 'discipline_with_year'
    offense_group = ('Learning Environment', 'Weapons', 'Personal/Physical Safety')
    aggregate_function = 'SUM'
    default_value = 0
    mode = 'last_n_years'
    n_years = 1
   
    @property
    def feature_id(self):
        return 164

    @property
    def feature_description(self):
        return 'discipline days in last year'     
    
    
class sum_discipline_weapons_last_year(AbstractDisciplineFeature):   
    
    table = 'discipline_with_year'
    offense_group = ('Weapons', 'foo')
    aggregate_function = 'SUM'
    default_value = 0
    mode = 'last_n_years'
    n_years = 1
   
    @property
    def feature_id(self):
        return 164

    @property
    def feature_description(self):
        return 'weapons related discipline days in last year'     
    

class sum_discipline_physical_last_year(AbstractDisciplineFeature):   
    
    table = 'discipline_with_year'
    offense_group = ('Personal/Physical Safety', 'foo')
    aggregate_function = 'SUM'
    default_value = 0
    mode = 'last_n_years'
    n_years = 1
   
    @property
    def feature_id(self):
        return 164

    @property
    def feature_description(self):
        return 'physical safety related discipline days in last year'    
    
    
class sum_discipline_learning_last_year(AbstractDisciplineFeature):   
    
    table = 'discipline_with_year'
    offense_group = ('Learning Environment', 'foo')
    aggregate_function = 'SUM'
    default_value = 0
    mode = 'last_n_years'
    n_years = 1
   
    @property
    def feature_id(self):
        return 164

    @property
    def feature_description(self):
        return 'learning environment related discipline days in last year'      
    

class sum_discipline_last_2_years(AbstractDisciplineFeature):   
    
    table = 'discipline_with_year'
    offense_group = ('Learning Environment', 'Weapons', 'Personal/Physical Safety')
    aggregate_function = 'SUM'
    default_value = 0
    mode = 'last_n_years'
    n_years = 2
   
    @property
    def feature_id(self):
        return 164

    @property
    def feature_description(self):
        return 'discipline days in last two years'     
    
    
class sum_discipline_weapons_last_2_years(AbstractDisciplineFeature):   
    
    table = 'discipline_with_year'
    offense_group = ('Weapons', 'foo')
    aggregate_function = 'SUM'
    default_value = 0
    mode = 'last_n_years'
    n_years = 2
   
    @property
    def feature_id(self):
        return 164

    @property
    def feature_description(self):
        return 'weapons related discipline days in last two years'     
    

class sum_discipline_physical_last_2_years(AbstractDisciplineFeature):   
    
    table = 'discipline_with_year'
    offense_group = ('Personal/Physical Safety', 'foo')
    aggregate_function = 'SUM'
    default_value = 0
    mode = 'last_n_years'
    n_years = 2
   
    @property
    def feature_id(self):
        return 164

    @property
    def feature_description(self):
        return 'physical safety related discipline days in last two years'    
    
    
class sum_discipline_learning_last_2_years(AbstractDisciplineFeature):   
    
    table = 'discipline_with_year'
    offense_group = ('Learning Environment', 'foo')
    aggregate_function = 'SUM'
    default_value = 0
    mode = 'last_n_years'
    n_years = 2
   
    @property
    def feature_id(self):
        return 164

    @property
    def feature_description(self):
        return 'learning environment related discipline days in last two years'      


class num_discipline_last_year(AbstractDisciplineFeature):   
    
    table = 'discipline_with_year'
    offense_group = ('Learning Environment', 'Weapons', 'Personal/Physical Safety')
    aggregate_function = 'COUNT'
    default_value = 0
    mode = 'last_n_years'
    n_years = 1
   
    @property
    def feature_id(self):
        return 164

    @property
    def feature_description(self):
        return 'number of discipline days in last year'     
    
    
class num_discipline_weapons_last_year(AbstractDisciplineFeature):   
    
    table = 'discipline_with_year'
    offense_group = ('Weapons', 'foo')
    aggregate_function = 'COUNT'
    default_value = 0
    mode = 'last_n_years'
    n_years = 1
   
    @property
    def feature_id(self):
        return 164

    @property
    def feature_description(self):
        return 'number of weapons related discipline days in last year'     
    

class num_discipline_physical_last_year(AbstractDisciplineFeature):   
    
    table = 'discipline_with_year'
    offense_group = ('Personal/Physical Safety', 'foo')
    aggregate_function = 'COUNT'
    default_value = 0
    mode = 'last_n_years'
    n_years = 1
   
    @property
    def feature_id(self):
        return 164

    @property
    def feature_description(self):
        return 'number of physical safety related discipline days in last year'    
    
    
class num_discipline_learning_last_year(AbstractDisciplineFeature):   
    
    table = 'discipline_with_year'
    offense_group = ('Learning Environment', 'foo')
    aggregate_function = 'COUNT'
    default_value = 0
    mode = 'last_n_years'
    n_years = 1
   
    @property
    def feature_id(self):
        return 164

    @property
    def feature_description(self):
        return 'number of learning environment related discipline days in last year'      
    

class num_discipline_last_2_years(AbstractDisciplineFeature):   
    
    table = 'discipline_with_year'
    offense_group = ('Learning Environment', 'Weapons', 'Personal/Physical Safety')
    aggregate_function = 'COUNT'
    default_value = 0
    mode = 'last_n_years'
    n_years = 2
   
    @property
    def feature_id(self):
        return 164

    @property
    def feature_description(self):
        return 'number of discipline days in last two years'     
    
    
class num_discipline_weapons_last_2_years(AbstractDisciplineFeature):   
    
    table = 'discipline_with_year'
    offense_group = ('Weapons', 'foo')
    aggregate_function = 'COUNT'
    default_value = 0
    mode = 'last_n_years'
    n_years = 2
   
    @property
    def feature_id(self):
        return 164

    @property
    def feature_description(self):
        return 'number of weapons related discipline days in last two years'     
    

class num_discipline_physical_last_2_years(AbstractDisciplineFeature):   
    
    table = 'discipline_with_year'
    offense_group = ('Personal/Physical Safety', 'foo')
    aggregate_function = 'COUNT'
    default_value = 0
    mode = 'last_n_years'
    n_years = 2
   
    @property
    def feature_id(self):
        return 164

    @property
    def feature_description(self):
        return 'number of physical safety related discipline days in last two years'    
    
    
class num_discipline_learning_last_2_years(AbstractDisciplineFeature):     
    table = 'discipline_with_year'
    offense_group = ('Learning Environment', 'foo')
    aggregate_function = 'COUNT'
    default_value = 0
    mode = 'last_n_years'
    n_years = 2
   
    @property
    def feature_id(self):
        return 164

    @property
    def feature_description(self):
        return 'number of learning environment related discipline days in last two years'      
    

class num_family_members(AbstractFeature):
    def __init__(self, table_name, conn):
        super(num_family_members, self).__init__(table_name, conn)    
        
    @property
    def feature_id(self):
        return 208
    @property
    def feature_col(self):
        return 'num_family_members' 
    @property
    def feature_type(self):  
        return 'numerical'

    @property
    def feature_description(self):
        return '''number of family members who interact with CJ'''

    def feature_code(self):
      rf_type, timeframe = self.extract_timeframe_from_table_name()
      if rf_type == 'year':
          sql = '''SELECT c.person_id, max(num_family_members) as num_family_members from 
             
                   (SELECT family_id, count(distinct person_id) as num_family_members 
                   from cj_schema.juv_case_person_id a group by family_id) as a
                   
                   join  cj_schema.juv_case_person_id  c using (family_id)  
                   RIGHT JOIN training.mapping b on c.person_id = b.person_id and inc_year <= {} 
                   group by 1'''.format(timeframe)
      data = pd.read_sql_query(sql, self.conn)
      data = pp.fill_null_with_zero(data, self.feature_col)
      return data, "num_family_members"

class num_chips_records(AbstractFeature):
    def __init__(self, table_name, conn):
        super(num_chips_records, self).__init__(table_name, conn)    
        
    @property
    def feature_id(self):
        return 209

    @property
    def feature_col(self):
        return 'num_chips_records' 

    @property
    def feature_type(self):  
        return 'numerical'

    @property
    def feature_description(self):
        return '''number of chips records up to a certain year'''

    def feature_code(self):
      rf_type, timeframe = self.extract_timeframe_from_table_name()
      if rf_type == 'year':
            sql = '''SELECT b.person_id, sum(case when chips is not null then 1 else 0 end) as num_chips_records
                        FROM cj_schema.juv_case_person_id a 
                         RIGHT JOIN training.mapping b on a.person_id = b.person_id and 
                               inc_year <= {}
                                  group by 1'''.format(timeframe)
      data = pd.read_sql_query(sql, self.conn)
      data = pp.fill_null_with_zero(data, self.feature_col)
      return data, "num_chips_records"


class best_score_reading_map_last_year(AbstractAssessmentAggregates):
    
    table = 'new_assessment_with_date'
    aggregate_function = 'MIN'
    test_subject = 'Reading'
    test_type = 'MAP SCREENER'
    n_years = 1
    test_primary_result_code = ('1', '2', '3', '4', '5')
    # 0: Not Applicable (dropped)
    # 1: Significantly above target 
    # 2: On target
    # 3: Below target
    # 4: Well below target
    # 5: Significantly below target
    # 6: Untested (dropped)
    
    default_value = 'M'

    @property
    def feature_id(self):
        return 100 

    @property
    def feature_description(self):
        return 'Best score in MAP SCREENER test, subject: Reading, last year'
    

class worst_score_reading_map_last_year(AbstractAssessmentAggregates):
    
    table = 'new_assessment_with_date'
    aggregate_function = 'MAX'
    test_subject = 'Reading'
    test_type = 'MAP SCREENER'
    n_years = 1
    test_primary_result_code = ('1', '2', '3', '4', '5')
    # 0: Not Applicable (dropped)
    # 1: Significantly above target 
    # 2: On target
    # 3: Below target
    # 4: Well below target
    # 5: Significantly below target
    # 6: Untested (dropped)
    
    default_value = 'M'

    @property
    def feature_id(self):
        return 100 

    @property
    def feature_description(self):
        return 'Worst score in MAP SCREENER test, subject: Reading, last year'    
    
class best_score_math_map_last_year(AbstractAssessmentAggregates):
    
    table = 'new_assessment_with_date'
    aggregate_function = 'MIN'
    test_subject = 'Mathematics'
    test_type = 'MAP SCREENER'
    n_years = 1
    test_primary_result_code = ('1', '2', '3', '4', '5')
    # 0: Not Applicable (dropped)
    # 1: Significantly above target 
    # 2: On target
    # 3: Below target
    # 4: Well below target
    # 5: Significantly below target
    # 6: Untested (dropped)
    
    default_value = 'M'

    @property
    def feature_id(self):
        return 100 

    @property
    def feature_description(self):
        return 'Best score in MAP SCREENER test, subject: Math, last year'
    

class worst_score_math_map_last_year(AbstractAssessmentAggregates):
    
    table = 'new_assessment_with_date'
    aggregate_function = 'MAX'
    test_subject = 'Mathematics'
    test_type = 'MAP SCREENER'
    n_years = 1
    test_primary_result_code = ('1', '2', '3', '4', '5')
    # 0: Not Applicable (dropped)
    # 1: Significantly above target 
    # 2: On target
    # 3: Below target
    # 4: Well below target
    # 5: Significantly below target
    # 6: Untested (dropped)
    
    default_value = 'M'

    @property
    def feature_id(self):
        return 100 

    @property
    def feature_description(self):
        return 'Worst score in MAP SCREENER test, subject: Math, last year'    
        
    
class best_score_reading_map_last_2_years(AbstractAssessmentAggregates):
    
    table = 'new_assessment_with_date'
    aggregate_function = 'MIN'
    test_subject = 'Reading'
    test_type = 'MAP SCREENER'
    n_years = 2
    test_primary_result_code = ('1', '2', '3', '4', '5')
    # 0: Not Applicable (dropped)
    # 1: Significantly above target 
    # 2: On target
    # 3: Below target
    # 4: Well below target
    # 5: Significantly below target
    # 6: Untested (dropped)
    
    default_value = 'M'
    
    @property
    def feature_id(self):
        return 100 

    @property
    def feature_description(self):
        return 'Best score in MAP SCREENER test, subject: Reading, last 2 years'


class worst_score_reading_map_last_2_years(AbstractAssessmentAggregates):
    
    table = 'new_assessment_with_date'
    aggregate_function = 'MAX'
    test_subject = 'Reading'
    test_type = 'MAP SCREENER'
    n_years = 2
    test_primary_result_code = ('1', '2', '3', '4', '5')
    # 0: Not Applicable (dropped)
    # 1: Significantly above target 
    # 2: On target
    # 3: Below target
    # 4: Well below target
    # 5: Significantly below target
    # 6: Untested (dropped)
    
    default_value = 'M'

    @property
    def feature_id(self):
        return 100 

    @property
    def feature_description(self):
        return 'Worst score in MAP SCREENER test, subject: Reading, last 2 years'    
    
    
class best_score_math_map_last_2_years(AbstractAssessmentAggregates):
    
    table = 'new_assessment_with_date'
    aggregate_function = 'MIN'
    test_subject = 'Mathematics'
    test_type = 'MAP SCREENER'
    n_years = 2
    test_primary_result_code = ('1', '2', '3', '4', '5')
    # 0: Not Applicable (dropped)
    # 1: Significantly above target 
    # 2: On target
    # 3: Below target
    # 4: Well below target
    # 5: Significantly below target
    # 6: Untested (dropped)
    
    default_value = 'M'

    @property
    def feature_id(self):
        return 100 

    @property
    def feature_description(self):
        return 'Best score in MAP SCREENER test, subject: Math, last 2 years'
    

class worst_score_math_map_last_2_years(AbstractAssessmentAggregates):
    
    table = 'new_assessment_with_date'
    aggregate_function = 'MAX'
    test_subject = 'Mathematics'
    test_type = 'MAP SCREENER'
    n_years = 2
    test_primary_result_code = ('1', '2', '3', '4', '5')
    # 0: Not Applicable (dropped)
    # 1: Significantly above target 
    # 2: On target
    # 3: Below target
    # 4: Well below target
    # 5: Significantly below target
    # 6: Untested (dropped)
    
    default_value = 'M'

    @property
    def feature_id(self):
        return 100 

    @property
    def feature_description(self):
        return 'Worst score in MAP SCREENER test, subject: Math, last 2 years'    
        
