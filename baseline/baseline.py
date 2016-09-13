import datetime
import pandas as pd
import psycopg2
import tempfile
from sqlalchemy import create_engine



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




user = 'milwaukee'
port = 5432 
host = 'postgres.dssg.io'

conn = create_engine('postgresql://{0}:{1}@{2}:{3}'.format(user, password, host, port))

sql_query = '''
             SELECT person_id, a.student_key, discipline_start_date as discipline_date, 
             --CASE when student_grade_code IN ('01','02','03','04','05','06','07','08') then 1 else end 0 as first_grades, 
             CASE when student_grade_code IN ('09','10','11','12') then 1 else 0 end as last_grades, 
             CASE WHEN discipline_state_action_group like %(my_regex)s then 1 else 0 end as suspension
             FROM edu_schema.discipline a
             RIGHT JOIN training.mapping b ON a.student_key=b.student_key and extract(year from discipline_start_date) = 2013
             JOIN edu_schema.new_demographic c on b.student_key=c.student_key and extract(year from collection_date) = 2012
             '''
df = pd.read_sql_query(sql_query, conn, params={'my_regex': '%Suspension%'})

df = df[df['last_grades']==1]
#young_students = df[df['first_grades']==1]
#older_students = df[df['last_grades']==1]


#Grades 9 through 12 if a student receives 3 Office Discipline Referrals (ODR) in 20 school days
num_incidents_necessary = 3 
smaller_df = df.groupby(['student_key']).filter(lambda x: len(x) >= num_incidents_necessary)
labels = set()
smaller_df.discipline_date = smaller_df.discipline_date.astype(str)
print (smaller_df['discipline_date'].dtype)
smaller_df.discipline_date = smaller_df.discipline_date.apply(lambda x: datetime.datetime.strptime(x, '%Y-%m-%d'))
twenty_eight_days = datetime.timedelta(days=28)
for (student_key), mini_df in smaller_df.groupby(['student_key']):
    for discipline_date in mini_df.discipline_date:
        if ((discipline_date - twenty_eight_days <= mini_df.discipline_date) &
            (mini_df.discipline_date <= discipline_date)).sum() >= num_incidents_necessary:
            labels.add(student_key)
        break

#Grades 9 through 12 if a student receives 2 out-of-school suspensions in 90 school days

df = df[df['suspension']==1]

num_incidents_necessary = 2
smaller_df = df.groupby(['student_key']).filter(lambda x: len(x) >= num_incidents_necessary)
smaller_df.discipline_date = smaller_df.discipline_date.astype(str)
smaller_df.discipline_date = smaller_df.discipline_date.apply(lambda x: datetime.datetime.strptime(x, '%Y-%m-%d'))
days = datetime.timedelta(days=100)
for (student_key), mini_df in smaller_df.groupby(['student_key']):
    for discipline_date in mini_df.discipline_date:
        if ((discipline_date - days <= mini_df.discipline_date) &
            (mini_df.discipline_date <= discipline_date)).sum() >= num_incidents_necessary:
            labels.add(student_key)
        break

print (len(labels))

new_df = pd.DataFrame({'labels': list(labels)}) 
print (new_df.shape)

new_df.to_sql(name='baseline_older_students', con=conn, schema='training', if_exists='replace', index=True, index_label=None, chunksize=None, dtype=None)
#copy_to_sql(new_df, 'training.baseline2', conn)
print ("done")
