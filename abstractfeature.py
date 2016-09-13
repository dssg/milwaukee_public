import abc
import pandas as pd
import preprocessing as pp
import random
import re
import tempfile


def copy_to_sql(df, table_name, engine):
    with tempfile.NamedTemporaryFile(mode='wt') as tmpfile:
        df.to_csv(tmpfile, index=False)
        tmpfile.flush()
        engine.execute(pd.io.sql.get_schema(df, 'something_9999999').replace('"something_9999999"', table_name))
        with engine.raw_connection().cursor() as curs, open(tmpfile.name, 'rt') as f:
            curs.copy_expert("""COPY {table_name}
                                FROM stdin
                                WITH CSV HEADER DELIMITER ',' QUOTE '"';
                            """.format(table_name=table_name), f)


def random_string(N):
    return ''.join(random.choice('abcdefghijklmnopqrstuvwxyz') for _ in range(N))


class AbstractFeature(object):
    '''
    Abstract representation of an feature.
    '''

    def __init__(self, table_name,conn):
        self.table_name = table_name
        self.conn = conn

        if self.feature_type not in ['boolean', 'numerical', 'categorical']:
            raise ValueError("Feature type must be 'boolean', 'numerical' or 'categorical'.")

    @abc.abstractproperty
    def feature_id(self):
        pass

    @abc.abstractproperty
    def feature_col(self):
        pass

    @abc.abstractproperty
    def feature_type(self):
        pass

    @abc.abstractproperty
    def feature_description(self):
        pass

    @abc.abstractproperty
    def postprocessors(self):
        pass

    @abc.abstractmethod
    def feature_code(self):
        pass

    def preprocessing(self, feature_df):
        if feature_df[self.feature_col].isnull().any():
            raise ValueError("Feature type cannot contains NULL.")
        #check negative values
        if self.feature_type=='numerical':
            if (feature_df[self.feature_col] < 0).any():
                print("WARNING: This feature contains negative values")
        #make sure there are no strings in numeric and boolean features
        if self.feature_type in ['numerical', 'boolean']:
            if feature_df[self.feature_col].dtype not in [float, int]:
                raise ValueError("Feature type which is not 'categorical' must contain"
                                 " only numeric values.")

        #check if the most common value in the feature appears in more than 80% of the data :
        if feature_df[self.feature_col].value_counts().max() > 0.8 * len(feature_df):
            print ("WARNING: {} is the common value in this feature. Its more than 80% of the data".format(feature_df[self.feature_col].value_counts().idxmax()))

        if not feature_df['person_id'].count() == feature_df['person_id'].nunique():
           print ("count = ", feature_df['person_id'].count())
           print ("unique = ", feature_df['person_id'].nunique())
           raise ValueError("This feature is not unique per student and about to damage your training-set!!!!!!!!! FIX IT")

        return feature_df

    def update_data_in_db(self, temp_table):
        """
        uploads the new feature table to the database as temp tables. The temp tables will be joined

        param conn: connection
        """
        # Convert booleans to something postgres will read
        temp_table = temp_table.copy()
        for column in temp_table.columns:
            if temp_table[column].dtype == bool:
                temp_table[column] = temp_table[column].apply(lambda x: 1 if x else 0)

        temp_table_name = 'temp_' + random_string(32)
        if self.feature_col == 'is_student_relevant':
            self.is_student_relevant_update(temp_table,temp_table_name)
        else:
            self.conn.execute("""
                 ALTER TABLE {schema}.{table_name} ADD COLUMN {feature_col} {feature_type};
                """.format(schema='training',
                       table_name=self.table_name,
                       feature_col=self.feature_col,
                       feature_type=self.feature_sql_type))
            copy_to_sql(temp_table, temp_table_name, self.conn)
            self.conn.execute("""
                    UPDATE {schema_name}.{table_name} tab
                    SET {feature_col} = temp_table.{feature_col}
                    FROM {temp_table_name} temp_table
                    WHERE tab.person_id = temp_table.person_id;
                    """.format(schema_name='training',
                       table_name=self.table_name,
                       feature_col=self.feature_col,
                       temp_table_name=temp_table_name))
            self.conn.execute("DROP TABLE {temp_table_name};".format(temp_table_name=temp_table_name))

    def extract_timeframe_from_table_name(self):
        timeframe = int(re.sub("\D", "", self.table_name))
        if timeframe > 1000:
            rf_type = 'year'
        else:
            rf_type = 'age'
        return rf_type, timeframe

    def run(self):
        feature_data, feature_name = self.feature_code()
        print("Ran the feature code {}".format(self.feature_col))
        feature_data = self.preprocessing(feature_data)
        print("Ran preprocessing code {}".format(self.feature_col))
        self.update_data_in_db(feature_data)
        print("Updated data in db for {}".format(self.feature_col))
        self.update_dictionary_in_db()
        print("Done with {}".format(self.feature_col))

    def update_dictionary_in_db(self):
        sql_query = 'INSERT INTO training.feature_dictionary VALUES (%s,%s,%s,%s); '
        self.conn.execute(sql_query, (self.feature_id, self.feature_col, self.feature_type, self.feature_description))

    @property
    def feature_sql_type(self):
        if self.feature_type == 'boolean':
            return 'integer'
        elif self.feature_type == 'numerical':
            return 'float'
        elif self.feature_type == 'categorical':
            return 'text'
        raise ValueError ("Feature type must be either boolean, numerical or categorical")

    def is_student_relevant_update(self, temp_table, temp_table_name):
        self.conn.execute('DROP TABLE IF EXISTS training.is_student_relevant')
        copy_to_sql(temp_table, 'training.is_student_relevant', self.conn)
        #temp_table.to_sql(name='is_student_relevant', con=self.conn, schema='training', if_exists='replace',
         #                 index=True, index_label=None, chunksize=None, dtype=None)

        sql_query_create = '''CREATE TABLE training.{temp_table_name} as (
                                SELECT a.*, b.is_student_relevant
                                  FROM training.is_student_relevant b
                                  JOIN training.{features_table} a
                                    ON a.person_id = b.person_id);'''.format(temp_table_name=temp_table_name,
                                                                           features_table=self.table_name)
        sql_query_drop_former = 'DROP TABLE training.{table};'.format(table=self.table_name)
        sql_query_alter_table_name = 'ALTER TABLE training.{temp_table_name} RENAME TO {features_table};'.format(temp_table_name = temp_table_name, features_table = self.table_name)
        sql_query_drop_if_exists = 'DROP TABLE IF EXISTS training.{temp_table_name}'.format(temp_table_name = temp_table_name)
        #cur = self.conn.cursor()
        self.conn.execute(sql_query_drop_if_exists)
        self.conn.execute(sql_query_create)
        self.conn.execute(sql_query_drop_former)
        self.conn.execute(sql_query_alter_table_name)


class AbstractAssessmentFeature(AbstractFeature):

    @property
    def feature_col(self):
        return self.__class__.__name__

    @property
    def feature_type(self):
        return 'categorical'

    def feature_code(self):
        _, test_year = self.extract_timeframe_from_table_name()
        return pd.read_sql_query("""
                    WITH test_scores AS (
                    SELECT student_key, {aggregate_func}(test_primary_result_code) AS prc
                    FROM edu_schema.{table}
                    WHERE test_subject = %(test_subject)s
                    AND test_type = %(test_type)s
                    AND test_primary_result_code IN %(test_primary_result_code)s
                    AND test_year <= %(test_year)s
                    GROUP BY student_key
                    )
                    SELECT person_id,
                    COALESCE(test_scores.prc, %(default_value)s) AS {feature_col}
                    FROM training.mapping
                    LEFT JOIN test_scores
                    ON test_scores.student_key = mapping.student_key
                    """.format(feature_col = self.feature_col, aggregate_func = self.aggregate_function, table = self.table), self.conn, params =
                     {'test_subject': self.test_subject,
                      'test_year': test_year,
                     'test_type': self.test_type,
                     'test_primary_result_code': self.test_primary_result_code,
                     'default_value': self.default_value}
                ), self.feature_col
class AbstractAssessmentAggregates(AbstractFeature):

    @property
    def feature_col(self):
        return self.__class__.__name__

    @property
    def feature_type(self):
        return 'categorical'

    def feature_code(self):
        _, test_year = self.extract_timeframe_from_table_name()
        return pd.read_sql_query("""
                    WITH test_scores AS (
                    SELECT student_key, {aggregate_function}(test_primary_result_code) AS prc
                    FROM edu_schema.{table}
                    WHERE test_subject = %(test_subject)s
                    AND test_type = %(test_type)s
                    AND test_primary_result_code IN %(test_primary_result_code)s
                    AND test_year <= %(test_year)s
                    AND test_year > %(test_year)s - %(n_years)s
                    GROUP BY student_key
                    )
                    SELECT person_id,
                    COALESCE(test_scores.prc, %(default_value)s) AS {feature_col}
                    FROM training.mapping
                    LEFT JOIN test_scores
                    ON test_scores.student_key = mapping.student_key
                    """.format(feature_col = self.feature_col, aggregate_function = self.aggregate_function, table = self.table), self.conn, params =
                     {'test_subject': self.test_subject,
                      'test_year': test_year,
                      'n_years': self.n_years,
                     'test_type': self.test_type,
                     'test_primary_result_code': self.test_primary_result_code,
                     'default_value': self.default_value}
                ), self.feature_col


class AbstractAssessmentSlope(AbstractFeature):

    @property
    def feature_col(self):
        return self.__class__.__name__

    @property
    def feature_type(self):
        return 'categorical'

    def feature_code(self):
        _, test_year = self.extract_timeframe_from_table_name()
        feature_data = pd.read_sql_query('''SELECT person_id, last_score - first_score as {feature_col}
                    FROM
                    (SELECT student_key, score as last_score
                    FROM
                    (SELECT student_key, cast(test_primary_result_code as int) as score, test_year,
                    row_number()
                    OVER (PARTITION BY n.student_key ORDER BY n.test_year desc) as rank1, count(*)
                    OVER (PARTITION BY n.student_key) as total_count
                    From edu_schema.{table} n
                    WHERE test_primary_result_code IN %(test_primary_result_code)s
                    AND test_type = %(test_type)s
                    AND test_subject = %(test_subject)s
                    AND test_year <= %(test_year)s) as foo
                    WHERE rank1 = total_count) as n

                    JOIN

                    (SELECT student_key, score as first_score
                    FROM (SELECT student_key, cast(test_primary_result_code as int) as score, test_year,
                    row_number()
                    OVER (PARTITION BY n.student_key ORDER BY n.test_year desc) as rank1
                    FROM edu_schema.{table} n
                    WHERE test_primary_result_code IN %(test_primary_result_code)s
                    AND test_type = %(test_type)s
                    AND test_subject = %(test_subject)s
                    AND test_year <= %(test_year)s) as foo
                    WHERE rank1 = 1) as b

                    ON n.student_key = b.student_key
                    RIGHT JOIN training.mapping d on b.student_key = d.student_key'''.format(feature_col = self.feature_col,
                    table = self.table), self.conn, params =
                    {'test_subject': self.test_subject,
                     'test_year': test_year,
                     'test_type': self.test_type,
                     'test_primary_result_code': self.test_primary_result_code})

        #binning
        feature_data[self.feature_col] = pd.cut(feature_data[self.feature_col], self.bins, labels = self.group_names)

        #filling-in null values
        feature_data = pp.fill_null_with_default_value(feature_data, self.feature_col, self.default_value)

        return feature_data, self.feature_col


class AbstractDisciplineFeature(AbstractFeature):

    @property
    def feature_col(self):
        return self.__class__.__name__

    @property
    def feature_type(self):
        return 'numerical'

    def feature_code(self):
        _, year = self.extract_timeframe_from_table_name()

        if self.mode == 'forever':
            feature = pd.read_sql_query("""
                    SELECT person_id, COALESCE(foo1, %(default_value)s) AS {feature_col} FROM (
                    SELECT person_id, a.student_key, {aggregate_function}(discipline_days) AS foo1
                    FROM edu_schema.{table} a
                    RIGHT JOIN training.mapping b ON a.student_key = b.student_key
                    AND a.discipline_year <= %(discipline_year)s
                    AND discipline_fed_offense_group IN %(offense_group)s
                    GROUP BY 1,2) AS foo2
                    """.format(feature_col = self.feature_col, aggregate_function = self.aggregate_function, table = self.table), self.conn, params =
                    {'discipline_year': year,
                     'offense_group': self.offense_group,
                     'default_value': self.default_value})

        elif self.mode == 'last_n_years':
            feature = pd.read_sql_query("""
                    SELECT person_id, COALESCE(foo1, %(default_value)s) AS {feature_col} FROM (
                    SELECT person_id, a.student_key, {aggregate_function}(discipline_days) AS foo1
                    FROM edu_schema.{table} a
                    RIGHT JOIN training.mapping b ON a.student_key = b.student_key
                    AND a.discipline_year <= %(discipline_year)s
                    AND a.discipline_year > %(discipline_year)s - %(n_years)s
                    AND discipline_fed_offense_group IN %(offense_group)s
                    GROUP BY 1,2) AS foo2
                    """.format(feature_col = self.feature_col, aggregate_function = self.aggregate_function, table = self.table), self.conn, params =
                    {'discipline_year': year,
                     'n_years': self.n_years,
                     'offense_group': self.offense_group,
                     'default_value': self.default_value})

        return feature, self.feature_col
