import pandas as pd
import warnings
import time
warnings.filterwarnings('ignore')

def true_false_setter(columns):
    if columns == 'null':
        return 0
    elif pd.isnull(columns):
        return 0
    else:
        return 1

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

        # Same reasoning with "invited_by_club", but if not invited by a club the user was invited by another user so we can set it
        # to 0 if NaN

    dataset['invited_by_club'] = dataset['invited_by_club'].fillna("No Club")

        # For further analysis support new features are going to be created.

        # Shows the date of creation
    dataset['date_created'] = pd.to_datetime(dataset['time_created'].str[0:10])

        # Auxiliar dataframe for calculating the number of profiles invited by that user
    invt_df = pd.DataFrame(dataset['invited_by_user_profile'].value_counts().rename_axis('user_id').reset_index(name='invite_count'))
    invt_df = invt_df[invt_df['user_id'] != 'null']

        # Add invite count to original dataset, useful for graph data managment
    dataset = dataset.merge(invt_df, how='left', on='user_id')
    dataset['invite_count'] = dataset['invite_count'].fillna(0)
    

    # Remove useless columns

    dataset.drop(['photo_url', 'time_created'], axis=1, inplace=True)

    print(dataset.info())
    print(dataset.head())
    
    edges_users_dataset = pd.DataFrame()
    edges_users_dataset['_from'] = dataset['invited_by_user_profile']
    edges_users_dataset['_to'] = dataset['user_id']
    
    edges_clubs_dataset = pd.DataFrame()
    edges_clubs_dataset['_from'] = dataset['invited_by_club']
    edges_users_dataset['_to'] = dataset['user_id']
    
    dataset.to_csv('data/clubhouse/user_data_processed.csv', index = False, encoding = 'utf-8')
    dataset.to_json('data/clubhouse/user_data_processed.json', orient='records', lines=True)
    edges_users_dataset.to_json('data/clubhouse/user_edges_processed.json', orient='records', lines=True)
    
    return edges_clubs_dataset



def clubs_preprocessing(edges_clubs_dataset):
    
    dataset = pd.read_csv("data/clubhouse/club_data.csv")
    print(dataset.info())
    
    # Remove useless columns
    
    dataset.drop(['photo_url', 'rules', 'description', 'url'], axis=1, inplace=True)
    
    print(dataset.info())
    print(dataset.head())
    
    # Check duplicates

    print('Unique club_id: {}'.format(dataset['club_id'].nunique()))
    print('Unique club name: {}'.format(dataset['name'].nunique()))
    print(dataset[dataset.duplicated('name', keep=False)])
    
    dataset.drop_duplicates(subset=['name'], keep='last', inplace=True)
    # Check NaN values

    print(dataset.isna().sum())

    # Change club features to 1/0 encoding for further analysis

    to_change = ['enable_private', 'is_follow_allowed', 'is_membership_private', 'is_community']
    for i in to_change:
        dataset[i] = dataset[i].apply(true_false_setter).astype('int')
        
    print(dataset.info())
    print(dataset.head())
    
    merged_dataset = pd.merge(left = dataset, right= edges_clubs_dataset, left_on='club_id', right_on='invited_by')
    edges_clubs_dataset['_from'] = merged_dataset['club_id']
    
    dataset.to_csv('data/clubhouse/club_data_processed.csv', index = False, encoding = 'utf-8')
    dataset.to_json('data/clubhouse/club_data_processed.json', orient='records', lines=True)
    edges_clubs_dataset.to_json('data/clubhouse/club_edges_processed.json', orient='records', lines=True)



def preprocessing_pipeline():
    
    start_time = time.time()
    users_preprocessing()
    clubs_preprocessing()
    print("--- Dataset preprocessed in %s seconds ---" % (time.time() - start_time))
    

if __name__ == '__main__':
    preprocessing_pipeline()
    
    ##DA creare nuovo file json che connette le persone e i club