from sklearn.ensemble import RandomForestClassifier
from sklearn.linear_model import LogisticRegression
import numpy as np
import pandas as pd
import random
import abstractfeature
import sys, csv

def gen_risk_score(X_train, y_train, X_test, test_set, i, config, conn):
    """geneate a risk score based on the model. Results include person_id, risk score, 
    first name, last name and birthdate, and is then exported to a csv file.
    """
    clf = RandomForestClassifier(n_estimators=1000, max_depth = 10, n_jobs=-1, min_samples_split=5, max_features = 'sqrt', random_state = i)
    probas_ = clf.fit(X_train, y_train).predict_proba(X_test)[:,1]
    test_index = np.argsort(probas_)[::-1]
    students = test_set['person_id']
    students_by_risk = students.iloc[test_index]
    probas = np.column_stack((students_by_risk, probas_[test_index]))
    probas = pd.DataFrame(probas)
    probas.columns = ['person_id','risk_score']
    random_string =list('abcdefghijklmnopqrstuvwxyz') 
    random.shuffle(random_string) 
    table_name = 'temp_' + ''.join(random_string);
    #generate a csv file name that incidates the feature and label sets
    csv_file_name = 'riskscore_' + ''.join(random_string) + 'fea08-10labely0910y1112.csv';
    abstractfeature.copy_to_sql(probas, table_name, conn)
    print('copy table success')
    sql = """
        SELECT a.*, c.student_first_name, c.student_last_name, c.student_birthdate
          FROM  {} a
            LEFT JOIN training.mapping b ON a.person_id = b.person_id 
              JOIN (select distinct(student_key), student_first_name, student_last_name, 
                student_birthdate from edu_schema.demographic) c ON b.student_key = c.student_key 
                  ORDER BY a.risk_score DESC, a.person_id DESC """.format(table_name)
    student_at_risk = pd.read_sql_query(sql, conn)
    student_at_risk.to_csv(csv_file_name)
    conn.execute("DROP TABLE {}".format(table_name))
    
    
    
