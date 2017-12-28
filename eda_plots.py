'''This module contains plots used in exploratory data analysis'''
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
import pdb
import seaborn as sns
import os
import pytz

def correlation_plots(df, x_col_name, y_col_name):
    '''This function makes scatter plots between the given columns, and provides
    correlation values'''
    n_df = drop_nans(df, [x_col_name, y_col_name])
    if not len(n_df[x_col_name]):
        print('no overlaping points between {} and {}'.format(x_col_name, y_col_name))
        return
    sns.set()
    x = n_df[x_col_name].values
    y = n_df[y_col_name].values
    saveloc = 'figures/correlation_plots/{}/'.format(x_col_name)
    check_dir(saveloc)
    name = '{}_vs_{}'.format(y_col_name, x_col_name)
    corr = np.corrcoef(x, y)[0][1]
    fig = plt.figure()
    plt.plot(x,y,'.',label='Correlation = {}'.format(corr), alpha=0.05)
    plt.xlabel(x_col_name)
    plt.ylabel(y_col_name)
    plt.title(name.replace('_', ' '))
    plt.legend()
    fig.savefig(saveloc+name+'.jpg')
    plt.close(fig)

def check_dir(location):
    '''This function checks if a location exists, and makes it if it doesn't.
    Only works for one layer.'''
    if not os.path.exists(location):
        os.makedirs(location)

def drop_nans(df, cols):
    '''This function returns a new dataframe with just the specified columns,
    dropping rows where there are no values'''
    n_df = df[cols].copy()
    return n_df.dropna()

def daily_plot(df,col_name):
    '''This function makes a plot with the course of each day plotted on top
    of each other'''
    if 'TIMESTAMP' in df.columns:
        phoenix = pytz.timezone('America/Phoenix')
        df['TIMESTAMP'] = df['TIMESTAMP'].apply(lambda x: x.tz_convert(phoenix))
        time = df['TIMESTAMP'].apply(lambda x: x.time())
    else:
        time = df.index.time
    val = df[col_name].copy().values
    sns.set()
    saveloc = 'figures/daily_plot/'
    check_dir(saveloc)
    name = '{}_vs_time'.format(col_name)
    fig = plt.figure()
    plt.plot(time,val,'.',alpha=0.01)
    plt.xlabel('Time')
    plt.ylabel(col_name)
    plt.title(name.replace('_', ' '))
    fig.savefig(saveloc+name+'.jpg')
    plt.close(fig)
