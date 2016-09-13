from __future__ import print_function
import json
import sys
import psycopg2
from sqlalchemy import create_engine
from dataloader import DataLoader
import magicloop as ml
import numpy as np
import pandas as pd
import datetime
import codecs
import yaml
import output
import click
import labels


@click.group()
def cli():
    """Milwaukee's main modeling magic"""
    pass

@cli.command('model')
@click.argument('credentials_file')
@click.argument('config_file')
def model_command(credentials_file, config_file):
    """Run all the models.

    CREDENTIALS_FILE points to db credentials as json. CONFIG_FILE points to model configurations as yml.
    """
    with open(credentials_file) as f:
        creds = json.load(f)

    with open(config_file) as c:
        config = yaml.load(c)

    conn = create_engine('postgresql://', connect_args=creds)
    clfs, grid = ml.define_clfs_params()
    X_train, y_train, X_test, y_test, train_set, test_set, keep_features = get_data(conn, config)
    results = ml.clf_loop(config, clfs, grid, X_train, y_train, X_test, y_test, train_set[keep_features].columns, conn)

@cli.command('risk_scores')
@click.argument('credentials_file')
@click.argument('config_file')
@click.option('--num-runs', '-n', type=int, default=10, help="Specifies the number of model runs that risk scores are averaged over.")

def make_risk_scores(credentials_file, config_file, num_runs):
    """Run a model and generate risk scores per student.

    CREDENTIALS_FILE points to db credentials as json.
    """
    with open(credentials_file) as f:
        creds = json.load(f)

    with open(config_file) as c:
        config = yaml.load(c)	

    conn = create_engine('postgresql://', connect_args=creds)
    X_train, y_train, X_test, y_test, train_set, test_set, keep_features = get_data(conn, config)
    for i in range(num_runs):
        output.gen_risk_score(X_train, y_train, X_test, test_set, i, config, conn)
        
def _does_label_exist_in_db(conn): 
    """check if labels table already exists, and if not, create it""" 
    sql_query = '''SELECT EXISTS (
                    SELECT 1
                    FROM   information_schema.tables 
                    WHERE  table_schema = 'training'
                    AND    table_name = 'labels')'''
    is_there = pd.read_sql(sql_query, conn)
    return is_there.exists.all()

def get_data(conn, config):
    """Given a connection to the database and a config specifying which
    data to grab, return the training and testing data.

    :param sqlalchemy.Engine conn: The connection to the database
    :param dict[str, object] config: The model run configuration
    :return: The training and testing data as X_train, y_train, X_test, y_test
    :rtype: (pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame)
    """
    
    if not _does_label_exist_in_db(conn):
        labels.gen_label(conn)
        
    data = DataLoader(config['features'], config['train_table'], config['test_table'], config['train_label'], config['test_label'], conn)
    label_set_train, label_set_test = data.load_label()
    feature_set_train, feature_set_test = data.load_data()
    train_set = pd.merge(feature_set_train, label_set_train, on = 'person_id')
    test_set = pd.merge(feature_set_test, label_set_test, on = 'person_id')


    keep_features = [col_name for col_name in train_set.columns
                     if col_name not in ['person_id',
                                         config['train_label'],
                                         config['test_label']]]
    sorted_columns = sorted(train_set.columns)
    train_set.sort(sorted_columns, inplace=True)
    X_train = train_set[keep_features].values
    y_train = train_set[config['train_label']]

    sorted_columns = sorted(test_set.columns)
    test_set.sort(sorted_columns, inplace=True)
    X_test = test_set[keep_features].values
    y_test = test_set[config['test_label']]

    print("Completed Data Loading")
    print("size of training set:", train_set.shape)
    print("train set columns", train_set.columns)
    print("size of test set:", test_set.shape)
    print("test set columns", test_set.columns)
    print("size of feature_set_train", feature_set_train.shape)

    return X_train, y_train, X_test, y_test, train_set, test_set, keep_features


if __name__ == "__main__":
    cli()
