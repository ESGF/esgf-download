import pandas as pd
from time import time


def prep_old_csv(old_csv):
    """
    fixes old path value in the old dataframe pre-merge
    :param old_csv: old dataframe from ENESDS csv
    :return: old df with proper path structure
    """
    for index, row in old_csv.iterrows():
        old_csv.at[index, 'path'] = row['path'].replace('/home/enes', '/home/jovyan')
    return old_csv


def main():
    old_df = pd.read_csv('ENESDS_CMIP6_OCT24.csv')
    del old_df['index']

    current_df = pd.read_csv('output.csv')
    del current_df['index']

    tock = time()
    old_df = prep_old_csv(old_df)
    full_df = pd.concat([current_df, old_df]).drop_duplicates().reset_index(drop=True)
    full_df.drop(full_df.index[full_df['time_range'] == 'gn3RaXbM42915'], inplace=True)
    full_df[['start_year', 'end_year']] = full_df[['start_year', 'end_year']].astype('int64', errors='ignore')

    full_df.to_csv('result.csv', index_label='index')
    tick = time()
    print('Processed {} rows in {}s'.format(len(full_df), tick - tock))


main()
