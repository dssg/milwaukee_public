import feature_generator
import pandas as pd
import preprocessing as pp
import itertools
import re

def flatten(iterable):
    '''this function flattens a list when the elements are either lists or primitives.
    Creates a generator not a list
    
    >>> total = 0
    >>> for x in flatten([[1, 2, 3], 4]):
    ...    total += x
    
    prints 10
    
    '''
    for x in iterable:
        if isinstance(x, list):
            for y in flatten(x):
                yield y
        else:
            yield x

class DataLoader(object):

    def __init__(self, feature_list, train_table, test_table,
                 train_label, test_label, conn, normalize=True):
        self.feature_list = feature_list
        self.train_label = train_label
        self.test_label = test_label
        self.train_table = train_table
        self.test_table = test_table
        self.conn = conn
        self.normalize = normalize

    def load_data(self):
        """
        checks if table_name exist, if not, create it. then feature exists in table_name. if not, generate feature.
        return dataframe of the features
        """
        
        table_list = [self.train_table, self.test_table]
        
        if 'is_student_relevant' not in self.feature_list:
            self.feature_list.append('is_student_relevant')

        for table in table_list:
            if not self._does_table_exist_in_db(table):
                print("creating feature table named", table)
                sql_query = '''create table training.{} as (select person_id from training.mapping)'''.format(table)
                self.conn.execute(sql_query)

            for feature in flatten(self.feature_list):             
                if not self._does_feature_exist_in_db(feature, table):
                    fn = getattr(feature_generator, feature)
                    fn1 = fn(table, self.conn)
                    fn1.run()
                else: 
                     print('feature {col_name} already exists in table {table_name}'.format(col_name = feature, table_name = table))

        print("loading data now!")

        sql_query_test = "select person_id, {feature_list} from training.{table};".format(
            feature_list=','.join(flatten(self.feature_list)), table=self.test_table)
        sql_query_train = "select person_id, {feature_list} from training.{table};".format(
            feature_list=','.join(flatten(self.feature_list)), table=self.train_table)
        
        features_df_test = pd.read_sql(sql_query_test, self.conn)
        features_df_train = pd.read_sql(sql_query_train, self.conn)
        
        sql_query = '''SELECT feature_name FROM training.feature_dictionary 
                       where feature_type = 'categorical' ''' 
        
        categorial_list = pd.read_sql(sql_query, self.conn)
        features_categorial_list = categorial_list.values.tolist()
        features_categorial_list = list(itertools.chain.from_iterable(features_categorial_list))
        print ('feature list provided,' , flatten(self.feature_list))
        print ('feature list in dictionary', features_categorial_list)
        final_list = list(set(features_categorial_list).intersection(flatten(self.feature_list)))

        print (final_list)
        features_df_test['is_train'] = 0
        features_df_train['is_train'] = 1
        all_features = pd.concat([features_df_test, features_df_train])

        if self.normalize:
            print("Normalizing features to [0, 1]...")
            for feature in all_features.columns:
                if not (feature in ('person_id', 'is_train') or feature in final_list):
                    max_val = all_features[feature].max()
                    min_val = all_features[feature].min()
                    the_range = max_val - min_val + 1e-4
                    all_features[feature] = (all_features[feature] - min_val) / (the_range)

        all_features = pp.get_dummies(all_features, columns=final_list)
        features_big_table_test = all_features[all_features['is_train'] == 0]
        features_big_table_train = all_features[all_features['is_train'] == 1]

        return features_big_table_train, features_big_table_test 
    
    def load_label(self):
        """
        This query returns the label as a dataframe of person_id and label
        """
        timeframe_train = int(re.sub("\D", "", self.train_table))
        timeframe_test = int(re.sub("\D", "", self.test_table))
        sql_query_train = "select person_id, {label} from training.labels where first_year_interaction >= {t_train}   ; ".format(label = self.train_label, t_train = timeframe_train)
        sql_query_test = "select person_id, {label} from training.labels where first_year_interaction >= {t_test} ; ".format(label = self.test_label, t_test= timeframe_test)
        
        label_df_train = pd.read_sql(sql_query_train, self.conn)
        label_df_test = pd.read_sql(sql_query_test, self.conn)
        
        return label_df_train, label_df_test
                         
    def _does_feature_exist_in_db(self, feature, table):
        """check if feature already exists in the features table
        params feature: name of feature
        params table: name of the table we want for this model
        return: True or False
        """
        sql_query = """SELECT EXISTS
                        (SELECT 1
                           FROM information_schema.columns
                          WHERE table_schema='training'
                            AND table_name=%(table_name)s
                            AND column_name=%(col_name)s);"""
        is_there = pd.read_sql_query(sql_query, self.conn,
                            params={"table_name": table,
                                             "col_name": feature})
        return is_there.exists.all()

    def _does_table_exist_in_db(self, table): 
        """check if tables for the required year already exists, and if not, create it""" 
        sql_query = '''SELECT EXISTS (
                        SELECT 1
                        FROM   information_schema.tables 
                        WHERE  table_schema = 'training'
                        AND    table_name = '{}')'''.format(table)
        is_there = pd.read_sql(sql_query, self.conn)
        return is_there.exists.all()



