'''This module contains functions related to feature engineering for the data'''
import pdb
import pandas as pd

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
