import pandas as pd
import numpy as np
import matplotlib.pyplot as plt


import dask.dataframe as dd
from dask.distributed import Client


def run2():
    import sqlite3

    connection = sqlite3.connect("profitability.db")
    crsr = connection.cursor()

    attachDatabaseSQL        = "ATTACH DATABASE ? AS acquisitions"

    dbSpec  = ("acquisitions.db",)

    crsr.execute(attachDatabaseSQL,dbSpec)
    crsr.execute('''SELECT * from pg1 left join acquisitions on pg1.ticker=acquisitions.ticker and pg1.date = acquisitions.date''')
    rows = crsr.fetchall()
    print(np.array(rows))
    djlfskjdl
    for row in rows:
        print(row)
    connection.commit()


def run3():
    client = Client()
    from dask_ml.datasets import make_classification
    df = dd.read_csv("isHealthTrain.csv",assume_missing=True,sample=640000000,blocksize="10MB")
    df = df.fillna(0).fillna(0)
    for column in df.columns:
        if '.' in column:
            df = df.drop(column,axis=1)
    # for column in droppedColumns:
    #     df = df.drop(column, axis=1)
    y_train = df['acquired']
    X_train = df.drop('acquired',axis=1)
    df2 = dd.read_csv("isHealthTest.csv",assume_missing=True,sample=640000000,blocksize="10MB")
    df2 = df2.fillna(0).fillna(0)
    for column in df2.columns:
        if '.' in column:
            df2 = df2.drop(column,axis=1)
    # for column in droppedColumns:
    #     df = df.drop(column, axis=1)
    y_test = df2['acquired']
    X_test = df2.drop('acquired',axis=1)
    # X_train,X_train2,y_train,y_train2 = train_test_split(X_train,y_train)
    x_test_tickers = X_test['ticker'].values.compute()
    x_test_dates = X_test['date'].values.compute()
    print(x_test_tickers[0])
    np.savetxt("x_test_tickers.csv",x_test_tickers,delimiter=",",fmt='%s')
    np.savetxt("x_test_dates.csv",x_test_dates,delimiter=",",fmt='%s')
    print("GOOD")
    for column in X_train.columns:
        if 'ticker' in column or 'date' in column:
            X_train = X_train.drop(column,axis=1)
            X_test = X_test.drop(column,axis=1)
    X_train = X_train.to_dask_array()
    X_test = X_test.values.compute()
    y_train = y_train.to_dask_array()
    y_test = y_test.values.compute()
    np.savetxt("y_test.csv", y_test, delimiter=",")
    from dask_ml.wrappers import Incremental
    from sklearn.linear_model import SGDClassifier
    from sklearn.neural_network import MLPClassifier
    from dask_ml.wrappers import ParallelPostFit

    est = MLPClassifier(solver='adam',activation='relu', random_state=0)
    print(est)
    inc = Incremental(est, scoring='f1')
    print("WORKING")
    for _ in range(10):
        inc.partial_fit(X_train, y_train, classes=[0,1])
        print("FITTED")
        np.savetxt("predictions.csv",inc.predict_proba(X_test))
        print('Score:', inc.score(X_test, y_test))


    # params = {'alpha': np.logspace(-2, 1, num=1000)}
    # from dask_ml.model_selection import IncrementalSearchCV
    # search = IncrementalSearchCV(est, params, n_initial_parameters=100,patience=20, max_iter=100)
    # search.fit(X_train, y_train, classes=[0, 1])
    # print(search)
    # print("SCORE")
    # print("FITTED")
    # np.savetxt("predictions.csv",search.predict_proba(X_test))
    # print('Score:', search.score(X_test, y_test))



def run():
    client = Client()
    from dask_ml.datasets import make_classification
    df = dd.read_csv("isHealth.csv",assume_missing=True,sample=640000000,blocksize="10MB")
    df = df.fillna(0).fillna(0)
    for column in df.columns:
        if '.' in column:
            df = df.drop(column,axis=1)
    # for column in droppedColumns:
    #     df = df.drop(column, axis=1)
    y = df['acquired']
    X = df.drop('acquired',axis=1)
    from dask_ml.model_selection import train_test_split
    X_train, X_test, y_train, y_test = train_test_split(X, y,test_size=.1)
    # X_train,X_train2,y_train,y_train2 = train_test_split(X_train,y_train)
    x_test_tickers = X_test['ticker'].values.compute()
    x_test_dates = X_test['date'].values.compute()
    print(x_test_tickers[0])
    np.savetxt("x_test_tickers.csv",[x_test_tickers,x_test_dates],delimiter=",",fmt='%s')
    np.savetxt("x_test_dates.csv",x_test_dates,delimiter=",",fmt='%s')
    print("GOOD")
    for column in X_train.columns:
        if 'ticker' in column or 'date' in column:
            X_train = X_train.drop(column,axis=1)
            X_test = X_test.drop(column,axis=1)
    X_train = X_train.to_dask_array()
    X_test = X_test.values.compute()
    y_train = y_train.to_dask_array()
    y_test = y_test.values.compute()
    np.savetxt("y_test.csv", y_test, delimiter=",")
    from dask_ml.wrappers import Incremental
    from sklearn.linear_model import SGDClassifier
    from sklearn.neural_network import MLPClassifier
    from dask_ml.wrappers import ParallelPostFit

    est = MLPClassifier(solver='adam',activation='relu', random_state=0)
    inc = Incremental(est, scoring='neg_log_loss')
    print("WORKING")
    for _ in range(10):
        inc.partial_fit(X_train, y_train, classes=[0,1])
        print("FITTED")
        np.savetxt("predictions.csv",inc.predict_proba(X_test))
        print('Score:', inc.score(X_test, y_test))


    # model = MLPClassifier(solver='sgd', hidden_layer_sizes=(10,2),random_state=1)
    params = {'alpha': np.logspace(-2, 1, num=1000)}
    from dask_ml.model_selection import IncrementalSearchCV
    search = IncrementalSearchCV(est, params, n_initial_parameters=100,patience=20, max_iter=100)
    search.fit(X_train, y_train, classes=[0, 1])
    print(search)
    print("SCORE")
    print("FITTED")
    np.savetxt("predictions.csv",inc.predict_proba(X_test))
    print('Score:', inc.score(X_test, y_test))

if __name__ == '__main__':
    run3()
