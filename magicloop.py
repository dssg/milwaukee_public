from __future__ import division
import pandas as pd
import numpy as np
from sklearn import preprocessing, cross_validation, svm, metrics, tree, decomposition, svm
from sklearn.ensemble import RandomForestClassifier, ExtraTreesClassifier, GradientBoostingClassifier, AdaBoostClassifier
from sklearn.linear_model import LogisticRegression, Perceptron, SGDClassifier, OrthogonalMatchingPursuit, RandomizedLogisticRegression
from sklearn.neighbors.nearest_centroid import NearestCentroid
from sklearn.naive_bayes import GaussianNB, MultinomialNB, BernoulliNB
from sklearn.tree import DecisionTreeClassifier
from sklearn.neighbors import KNeighborsClassifier
from sklearn.cross_validation import train_test_split
from sklearn.grid_search import ParameterGrid
from sklearn.metrics import *
from sklearn.preprocessing import StandardScaler
import random
import pylab as pl
import matplotlib.pyplot as plt
from scipy import optimize
import time
import yaml
import json
import sys


def define_clfs_params():

    clfs = {'RF': lambda: RandomForestClassifier(n_estimators=50, n_jobs=-1),
        'ET': lambda: ExtraTreesClassifier(n_estimators=10, n_jobs=-1, criterion='entropy'),
        'AB': lambda: AdaBoostClassifier(DecisionTreeClassifier(max_depth=1), algorithm="SAMME", n_estimators=200),
        'LR': lambda: LogisticRegression(penalty='l1', C=1e5),
        'SVM': lambda: svm.SVC(kernel='linear', probability=True, random_state=0),
        'GB': lambda: GradientBoostingClassifier(learning_rate=0.05, subsample=0.5, max_depth=6, n_estimators=10),
        'NB': lambda: GaussianNB(),
        'DT': lambda: DecisionTreeClassifier(),
        'SGD': lambda: SGDClassifier(loss="hinge", penalty="l2"),
        'KNN': lambda: KNeighborsClassifier(n_neighbors=3)
            }

    grid = {
    'RF':{'n_estimators': [1,10,100], 'max_depth': [1,5,10,20],
          'max_features': ['sqrt','log2',0.33],'min_samples_split': [2,5,10],'criterion':['gini','entropy']},
    'LR': { 'penalty': ['l1','l2'],'C': [0.00001,0.0001,0.001,0.01,0.1,1,10]},
    'SGD': { 'loss': ['hinge','log','perceptron'], 'penalty': ['l2','l1','elasticnet']},
    'ET': { 'n_estimators': [1,10,100], 'criterion' : ['gini', 'entropy'] ,
           'max_depth': [1,5,10,20,50,100], 'max_features': ['sqrt','log2'],'min_samples_split': [2,5,10]},
    'AB': { 'algorithm': ['SAMME', 'SAMME.R'], 'n_estimators': [1,10,100,1000]},
    'GB': {'n_estimators': [1,10,100,1000], 'learning_rate' : [0.001,0.01,0.05,0.1,0.5],
           'subsample' : [0.1,0.5,1.0], 'max_depth': [1,3,5,10,20,50,100]},
    'NB' : {},
    'DT': {'criterion': ['gini', 'entropy'], 'max_depth': [1,5,10,20,50,100],
           'max_features': ['sqrt','log2'],'min_samples_split': [2,5,10]},
    'SVM' :{'C' :[0.00001,0.0001,0.001,0.01,0.1,1,10],'kernel':['linear']},
    'KNN' :{'n_neighbors': [1,5,10,25,50,100],'weights': ['uniform','distance'],
            'algorithm': ['auto','ball_tree','kd_tree']}
           }
    return clfs, grid

def clf_loop(config, clfs, grid, X_train, y_train,  X_test, y_test, feature_list, conn):
    models_to_run = config['models_to_run']
    result_dump = []
    k = config['k']
    for n in range(1, 2):
        #X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=0)
        for index,clf_factory in enumerate([clfs[x] for x in models_to_run]):
            print(models_to_run[index])
            parameter_values = grid[models_to_run[index]]
            for p in ParameterGrid(parameter_values):
                try:
                    clf = clf_factory()
                    clf.set_params(**p)
                    print(clf)
                    if hasattr(clf, 'predict_proba'):
                        y_pred_probs = clf.fit(X_train, y_train).predict_proba(X_test)[:,1]
                    else:
                        y_pred_probs = clf.fit(X_train, y_train).decision_function(X_test)
                    #threshold = np.sort(y_pred_probs)[::-1][int(.05*len(y_pred_probs))]
                    #print threshold
                    recall = recall_at_k(y_test, y_pred_probs, k)
                    precision = precision_at_k(y_test, y_pred_probs, k)
                    auc = calc_auc(y_test, y_pred_probs)
                    conf_mat = calc_confusion_matrix(y_test, y_pred_probs, k)

                    result = {"clf": clf, "recall": recall, "precision": precision, "auc": auc, "confusion_matrix": conf_mat}
                    write_result_to_db(clf, config, result, X_train, X_test,y_train, y_test, feature_list, conn)

                    plot_precision_recall_n(y_test, y_pred_probs, clf, config['output_plot'])
                except IndexError as e:
                    raise
                    print('Error:',e)
                    continue


def write_result_to_db(clf, config, result, X_train, X_test, y_train, y_test, feature_list, conn):
    '''
    Write model results to dataframe, then append it to the results database
    '''
    if hasattr(clf, 'feature_importances_'):
        importances = clf.fit(X_train, y_train).feature_importances_
    elif hasattr(clf, 'coef_'):
        importances = clf.fit(X_train, y_train).coef_
        importances = importances[0]

    indices = np.argsort(importances)[::-1]
    actual_feature_list = feature_list
    feature_byrank = actual_feature_list[indices]
    feature_importance_df = pd.DataFrame({ 'feature': feature_byrank, 'importance': importances[indices]})


    model_df = pd.DataFrame.from_records([[  list(flatten(config['features'])), config['train_table'], config['test_table'], config['train_label'], config['test_label'], str(X_train.shape), str(X_test.shape),  json.dumps(str(result['clf'])), config['k'], result['precision'],  result['auc'], result['recall'], str(result['confusion_matrix'])]] , columns=['features', 'train_set','test_set', 'train_label', 'test_label', 'train_size','test_size', 'clf_params', 'k', 'precision', 'auc', 'recall','cm'])

    for _, row in model_df.iterrows():
        id_df = pd.read_sql_query("""INSERT INTO training.models
         ({raw_name}) VALUES ({wrapped_name}) RETURNING id""".format(raw_name=','.join(model_df.columns),
                         wrapped_name=','.join('%({})s'.format(x) for x in model_df.columns)), conn, params=dict(row))
        the_id = id_df['id'][0]
        feature_importance_df['model_id'] = the_id

    feature_importance_df.to_sql('feature_importance', conn, schema='training', if_exists = 'append', index = False )


def plot_precision_recall_n(y_true, y_prob, model_name, config):
    from sklearn.metrics import precision_recall_curve
    y_score = y_prob
    precision_curve, recall_curve, pr_thresholds = precision_recall_curve(y_true, y_score)
    precision_curve = precision_curve[:-1]
    recall_curve = recall_curve[:-1]
    pct_above_per_thresh = []
    number_scored = len(y_score)
    for value in pr_thresholds:
        num_above_thresh = len(y_score[y_score>=value])
        pct_above_thresh = num_above_thresh / float(number_scored)
        pct_above_per_thresh.append(pct_above_thresh)
    pct_above_per_thresh = np.array(pct_above_per_thresh)
    #plt.clf()
    fig, ax1 = plt.subplots()
    ax1.plot(pct_above_per_thresh, precision_curve, 'b')
    ax1.set_xlabel('percent of population')
    ax1.set_ylabel('precision', color='b')
    ax2 = ax1.twinx()
    ax2.plot(pct_above_per_thresh, recall_curve, 'r')
    ax2.set_ylabel('recall', color='r')

    name = model_name
    plt.title(name)
    plt.savefig('week12.png')
    plt.show()



def precision_at_k(y_true, y_scores, k):
    threshold = np.sort(y_scores)[::-1][int(k*len(y_scores))]
    y_pred = np.asarray([1 if i >= threshold else 0 for i in y_scores])
    return metrics.precision_score(y_true, y_pred)


def calc_auc(y_test, y_pred_probs):
    return metrics.roc_auc_score(y_test, y_pred_probs)


def recall_at_k(y_true, y_scores, k):
    threshold = np.sort(y_scores)[::-1][int(k*len(y_scores))]
    y_pred = np.asarray([1 if i >= threshold else 0 for i in y_scores])
    return metrics.recall_score(y_true, y_pred)


def calc_confusion_matrix(y_true, y_scores, k):
    threshold = np.sort(y_scores)[::-1][int(k*len(y_scores))]
    y_pred = np.asarray([1 if i >= threshold else 0 for i in y_scores])
    return metrics.confusion_matrix(y_true, y_pred)

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
