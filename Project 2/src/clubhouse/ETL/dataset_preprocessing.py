import pandas as pd
import warnings
import time

from pandas.core.frame import DataFrame
warnings.filterwarnings('ignore')

def true_false_setter(columns):
    if columns == 'null':
        return 0
    elif pd.isnull(columns):
        return 0
    else:
        return 1

def edge_preprocessing():
    
    df_1 = DataFrame()
    df_2 = DataFrame()
    
    df_1 = pd.read_csv('data/clubhouse/user_data_processed.csv')
    df_2 = pd.read_csv('data/clubhouse/user_data_processed.csv')
    
    df_1 = df_1.drop(labels=['name', 'username', 'twitter', 'instagram', 'num_followers', 'num_following', 'date_created', 'invite_count'], axis=1)
    df_2 = df_2.drop(labels=['name', 'username', 'twitter', 'instagram', 'num_followers', 'num_following', 'date_created', 'invite_count'], axis=1)
    print(df_1.head())
    
    df_merge = DataFrame()
    
    df_merge = pd.merge(df_1, df_2, left_on=['user_id'], right_on = ['invited_by_user_profile'], how='inner')
    print(df_merge.head())
    
    edges = DataFrame()
    edges['_from'] = df_merge['_key_x']
    edges['_to'] =df_merge['_key_y']
    
    edges.to_json('data/clubhouse/edge_data_processed.json', orient='records', lines=True)


def users_preprocessing():

    dataset = pd.read_csv("data/clubhouse/user_data.csv")
    print(dataset.info())

    # Check duplicates

    print('Unique user_id: {}'.format(dataset['user_id'].nunique()))
    print('Unique username: {}'.format(dataset['username'].nunique()))

    dataset[dataset.duplicated('username', keep=False)]

    # Check NaN values

    print(dataset.isna().sum())

      # Change social features to 1/0 encoding for further analysis

    to_change = ['twitter', 'instagram']
    for i in to_change:
        dataset[i] = dataset[i].apply(true_false_setter).astype('int')

        # invited_by_user_profile and invited_by_club are our relationship features
        # If NaN we have to encode them in a specific label. If "invited_by_user_profile" is NaN then it means this profile was one of
        # the original ones. It can be set to 0.

    dataset['invited_by_user_profile'] = dataset['invited_by_user_profile'].fillna(0)

        # For further analysis support new features are going to be created.

        # Shows the date of creation
    dataset['date_created'] = pd.to_datetime(dataset['time_created'].str[0:10])

        # Auxiliar dataframe for calculating the number of profiles invited by that user
    invt_df = pd.DataFrame(dataset['invited_by_user_profile'].value_counts().rename_axis('user_id').reset_index(name='invite_count'))
    invt_df = invt_df[invt_df['user_id'] != 'null']

        # Add invite count to original dataset, useful for graph data managment
    dataset = dataset.merge(invt_df, how='left', on='user_id')
    dataset['invite_count'] = dataset['invite_count'].fillna(0).astype('int')
    
    # Remove useless columns
    
    dataset.drop(['photo_url', 'time_created', 'invited_by_club'], axis=1, inplace=True)
    
    dataset['index'] = dataset.index
    dataset['index'] = dataset['index'].astype(str)
    dataset['_key'] = (dataset['index'].astype(int)).astype(str)
    dataset['_key'] = dataset['_key'] + ':' + dataset['user_id'].astype(str)
    dataset['invited_by_user_profile'] = (dataset['invited_by_user_profile'].astype(int)).astype(str)
    
    print(dataset.info())
    print(dataset.head())

    dataset.to_csv('data/clubhouse/user_data_processed.csv', index = False, encoding = 'utf-8')
    dataset.to_json('data/clubhouse/user_data_processed.json', orient='records', lines=True)


def preprocessing_pipeline():
    
    start_time = time.time()
    users_preprocessing()
    edge_preprocessing()
    print("--- Dataset preprocessed in %s seconds ---" % (time.time() - start_time))
    

if __name__ == '__main__':
    preprocessing_pipeline()