import pandas as pd
from sqlalchemy import create_engine
import pdb
import feature_engineering
import secret
import pytz
import numpy as np

def loader(month, day):
    '''This function returns a dataframe with the information for that month loaded'''
    name = 'data/ASUPRL_{} 2017.xlsx'.format(month)
    df = pd.read_excel(name, sheetname=day, header=1, skiprows=[2,3])
    return df

def make_numeric(df):
    '''This function converts objects in a dataframe to numeric with NaNs where
    data is missing'''
    columns = df.columns
    for column in columns:
        if df[column].values.dtype == 'O':
            df[column] = pd.to_numeric(df[column], errors='coerce')

def drop_bad_points(df):
    '''This function drops the points which arose due to discrepencies in the
    Excell files, and fixes the wind data. Problems noted in the data:

    There are 1097 lines with extra 90s at the end of the WindDir_D1 columns -
        presumably from the addition of 90 to the column to make things better?
    September 23, 24, and 25 are missing from the excel document.
    October 17 has three additional points for some temperature data, specifically
        AirTC_Max, AIRTC_Min, AirTC_Avg, BattV_Min, GHI_W_Avg, GHI_W_Max, LAT_W_Avg,
        LAT_W_Max, NOCT_W_Avg, and NOCT_W_Max, RECORD.1, RH, Rain_mm_Tot, WindDir_SD1_WVT,
        WindDir_D1_WVT, and WS_ms_S_WVT.
    On October 17, Column A of the excel table jumps 2 minutes instead of 30 seconds
        at 13:04:30
    Days with mismatched timestamps: October 14 - 22

    June 20 is missing 107 AirTC_Avg data points between 8 and 9 AM

    RECORD and RECORD.1 are never equal to each other?

    WindDir_D1 and WindDir_D1_WVT do not have complete data

    October 4th has a couple lines without Temperature data, but with diffV for
        some of the cells. They are at 11:41:30 and 11:42:00

    October 22 has missing RECORD.1 data
    '''
    #Remove rows with no timestamp
    n_df = df.dropna(axis=0, subset=['TIMESTAMP'])
    #Set timestamp as index
    n_df.set_index('TIMESTAMP', inplace=True)
    #Remove rows with mismatched timestamps (October 14-22)
    n_df = n_df.drop(n_df[n_df.index != n_df['TIMESTAMP.1']].index)
    #Remove rows without AirTC database (June 20, 8-9 AM)
    n_df = n_df.drop(n_df[n_df['AirTC_Avg'].isnull()].index)
    #Drop two rows without most cell data
    n_df = n_df.drop(n_df[n_df['Temp_0_In_Avg'].isnull()].index)
    #Drop WindDir_D1_WVT > 360 rows
    n_df = n_df.drop(n_df[n_df['WindDir_D1_WVT'] > 360].index)
    #Drop unnamed columns and extra timestamp
    n_df = n_df.drop(['Unnamed: 30', 'Unnamed: 31', 'Unnamed: 48', 'Unnamed: 50', 'TIMESTAMP.1'],axis=1)
    #Drop rows with wind speed = 0
    n_df = n_df.drop(n_df[n_df['WS_ms_S_WVT'] == 0].index)
    return n_df

def set_timezones(df):
    '''This function sets the timezone for datetime objects'''
    phoenix = pytz.timezone('America/Phoenix')
    df['TIMESTAMP'] = df['TIMESTAMP'].apply(lambda x: x.tz_convert(phoenix))
    df['TIMESTAMP.1'] = df['TIMESTAMP.1'].apply(lambda x: x.tz_localize(phoenix))
    df['TIMESTAMP'] = df['TIMESTAMP'].apply(lambda x: pd.to_datetime(str(x)))
    df['TIMESTAMP.1'] = df['TIMESTAMP.1'].apply(lambda x: pd.to_datetime(str(x)))

def load_from_excel():
    months = ['May', 'June', 'July', 'August', 'September', 'October']
    master_df = pd.DataFrame([])
    max_days = [31, 30, 31, 31, 30, 31]
    for ind,month in enumerate(months):
        mdays=[]
        print('loading {}'.format(month))
        for day in range(1,max_days[ind]+1):
            if month=='May':
                mday = '{0} {1:02d}'.format(month, day)
                mdays.append(mday)
            elif len(month)>4:
                if month == 'September' and day in set([23,24,25]):
                    pass
                else:
                    mday = '{0} {1}'.format(month[0:3], day)
                    mdays.append(mday)
            else:
                mday = '{0} {1}'.format(month, day)
                mdays.append(mday)
        dfs = loader(month, mdays)
        print('concatenating days')
        for key in dfs.keys():
            df = dfs[key]
            master_df = pd.concat([master_df,df])
    return master_df

def add_true_wind_dir(df):
    '''This combines the WindDir_D1_WVT and WindDir_D1 into a single column,
    according to which is supposed to be used'''
    df['WindDir_D1_WVT'] = np.where(df['WindDir_D1'].notnull(), df['WindDir_D1'],
            df['WindDir_D1_WVT'])
    df.drop('WindDir_D1', axis=1, inplace=True)

def main():
    usr, pwd, serv, db = secret.sql_loading()
    engine = create_engine('postgresql://{usr}:{pwd}@{serv}/{db}'.format(usr=usr,
                                                                        pwd=pwd,
                                                                        serv=serv,
                                                                        db=db))

    master_df = load_from_excel()
    print('Anonymizing data')
    secret.rename_columns(master_df)
    print('Making numeric')
    make_numeric(master_df)
    # print('Standardizing timestamps')
    # set_timezones(master_df)
    print('Adding Deltas')
    feature_engineering.add_temp_delta(master_df)
    print('Cleaning Data')
    master_df = drop_bad_points(master_df)
    print('Fixing Wind Data')
    add_true_wind_dir(master_df)
    print('saving to sql')
    master_df.to_sql('clean', engine, if_exists='replace')
    print('done')

if __name__ == '__main__':
    main()
