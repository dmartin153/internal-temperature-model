import pandas as pd
from sqlalchemy import create_engine
import pdb
import feature_engineering
import secret

def loader(month, day):
    '''This function returns a dataframe with the information for that month loaded'''
    name = 'data/ASUPRL_{} 2017.xlsx'.format(month)
    df = pd.read_excel(name, sheetname=day, header=1, skiprows=[2,3], index_col='TIMESTAMP')
    return df

def make_numeric(df):
    '''This function converts objects in a dataframe to numeric with NaNs where
    data is missing'''
    columns = df.columns
    for column in columns:
        if df[column].values.dtype == 'O':
            df[column] = pd.to_numeric(df[column], errors='coerce')

def main():
    months = ['May', 'June', 'July', 'August', 'September', 'October']
    usr, pwd, serv, db = secret.sql_loading()
    engine = create_engine('postgresql://{usr}:{pwd}@{serv}/{db}'.format(usr=usr,
                                                                        pwd=pwd,
                                                                        serv=serv,
                                                                        db=db))
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
    print('Anonymizing data')
    secret.rename_columns(master_df)
    print('Making numeric')
    make_numeric(master_df)
    print('Adding Deltas')
    feature_engineering.add_temp_delta(master_df)
    print('saving to sql')
    master_df.to_sql('master', engine, if_exists='replace')
    print('done')
if __name__ == '__main__':
    main()
