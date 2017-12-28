'''This module contains functions related to feature engineering for the data'''
import pdb
import pandas as pd
from sklearn.linear_model import LinearRegression
from sklearn.model_selection import train_test_split
import numpy as np

def add_temp_delta(df):
    '''This function commbines out and in temperature readings to find the difference
    in temperature between the backend and middle of the cell'''
    columns = df.columns
    numbers = [0, 1, 2, 3, 4, 5]
    nums_with_multiple = [8, 13, 14]
    locations = ['Center', 'Corner', 'Edge']
    for num in numbers:
        interior = pd.to_numeric(df['Temp_{}_In_Avg'.format(num)], errors='coerce')
        exterior = pd.to_numeric(df['Temp_{}_Out_Avg'.format(num)], errors='coerce')
        df['Temp_{}_Delta'.format(num)] = interior - exterior
    for num in nums_with_multiple:
        for location in locations:
            interior = df['Temp_{}_In_{}_Avg'.format(num, location)]
            exterior = df['Temp_{}_Out_{}_Avg'.format(num,location)]
            df['Temp_{}_{}_Delta'.format(num,location)] = interior - exterior

def basic_linear_model(df,panel_num=1):
    '''This function builds a basic linear model for internal temperature. Assumes
    only training data is included in the dataframe.'''
    model = LinearRegression()
    cols = df.columns.drop(['TIMESTAMP','Temp_{}_Delta'.format(panel_num),
                            'Temp_{}_Out_Avg'.format(panel_num),
                            'Temp_{}_In_Avg'.format(panel_num)])
    y = df['Temp_{}_Out_Avg'.format(panel_num)].values
    df_norm = (df[cols] - df[cols].mean()) / (df[cols].max() - df[cols].min())
    X = np.insert(df_norm.values, 0, 1, axis=1)
    model.fit(X,y)
    return model, X, y

    '''Results from panel_num = 1:
    After normalizing data and fitting the Out average temperature, the most
    relevant column was NOCT_W_Avg, followed by AirTC_Avg. Both were directly
    correlated. model.coef_ values:
        AirTC_AVg: 35.59
        DiffV_1_Avg: 1.47
        NOCT_W_Avg: 37.11
        RH: 4.00
        Rain_mm_Tot: -0.02
        WS_ms_S_WVT: -4.323
        WindDir_D1_WVT: -0.36
    To interpret each of these: Ambient temperature has a high impact, as does irradiance.
    Wind speed decreases it, and the wind direction is in the wrong format to
    be impactful. Rain rarely happens, so is negligable. Relative humidity is odd,
    because we would expect it to be negatively correlated. I don't know what
    DiffV is, so am unsure why it should or shouldn't be relevant.

    This model has a R2 score of 0.8842. A visual inspection of the prediction and
    the targets indicates that the data is shifted up a bit, but has a
    roughly correct shape for each day.'''
