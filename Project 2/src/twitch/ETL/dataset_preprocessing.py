import pandas as pd
import warnings
import time

from pandas.core.frame import DataFrame
warnings.filterwarnings('ignore')

def arango_features_preprocessing():
    
    dataset = pd.read_csv("data/twitch/large_twitch_features.csv")
    print(dataset.head())
    print(dataset.info())
    
    # Check NaN values
    print(dataset.isna().sum())
    dataset = dataset.rename(columns={'language': '_key'})
    dataset['language'] = dataset['_key'].astype(str)
    dataset['_key'] = (dataset['_key'].astype(str) + ':' + dataset['numeric_id'].astype(str)).astype(str)
    dataset['numeric_id'] = 'userId_' + dataset['numeric_id'].astype(str)
     
    dataset.to_json('data/twitch/large_twitch_features_processed.json', orient='records', lines=True)
    
    return dataset

    
def arango_edge_preprocessing(dataset_key):
    
    dataset = pd.read_csv("data/twitch/large_twitch_edges.csv")
    
    print(dataset.head())
    
    dataset = dataset.rename(columns={'numeric_id_1': '_from', 'numeric_id_2': '_to'})  
    dataset = dataset.sort_values('_from')
    
    dataset['_from'] = dataset['_from'].astype(str)
    dataset['_to'] = dataset['_to'].astype(str)
    dataset['_to'] = 'userId_' + dataset['_to'].astype(str)
    dataset['_from'] = 'userId_' + dataset['_from'].astype(str)
    df = DataFrame()
    df = pd.merge(dataset, dataset_key, left_on=['_from'], right_on = ['numeric_id'], how='outer')
    
    df['_from'] = df['_key']
    df2 = pd.merge(dataset, dataset_key, left_on=['_to'], right_on = ['numeric_id'], how='outer')
    df2['_to'] = df2['_key']
    
    print(df.head())
    print(df2.head())    

    dataset['_from'] = df['_from']
    dataset['_to'] = df2['_to']
    
    
    print(dataset.head())
    
    # Check NaN values

    print(dataset.isna().sum())
    print(dataset.info())
    
    dataset.to_json('data/twitch/large_twitch_edges_processed.json', orient='records', lines=True)


def preprocessing_pipeline():
    
    start_time = time.time()
    data = arango_features_preprocessing()
    arango_edge_preprocessing(data)
    print("--- Dataset preprocessed in %s seconds ---" % (time.time() - start_time))
    

if __name__ == '__main__':
    preprocessing_pipeline()