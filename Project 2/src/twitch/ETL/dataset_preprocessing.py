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

def arango_features_preprocessing():
    
    dataset = pd.read_csv("data/twitch/large_twitch_features.csv")
    print(dataset.head())
    print(dataset.info())
    
    # Check NaN values
    print(dataset.isna().sum())
    dataset = dataset.rename(columns={'numeric_id': '_key'})
    dataset['_key'] = 'userId_' + dataset['_key'].astype(str)
     
    dataset.to_csv('data/twitch/large_twitch_features_processed.csv', index = False, encoding = 'utf-8')
    dataset.to_json('data/twitch/large_twitch_features_processed.json', orient='records', lines=True)

    
def arango_edge_preprocessing():
    
    dataset = pd.read_csv("data/twitch/large_twitch_edges.csv")
    print(dataset.head())
    
    dataset = dataset.rename(columns={'numeric_id_1': '_from', 'numeric_id_2': '_to'})  
    dataset = dataset.sort_values('_from')
    dataset['_from'] = 'userId_' + dataset['_from'].astype(str)
    dataset['_to'] = 'userId_' + dataset['_to'].astype(str)
    print(dataset.head())
    
    # Check NaN values

    print(dataset.isna().sum())
    print(dataset.info())
    
    dataset.to_csv('data/twitch/large_twitch_edges_processed.csv', index = False, encoding = 'utf-8')
    dataset.to_json('data/twitch/large_twitch_edges_processed.json', orient='records', lines=True)


def preprocessing_pipeline():
    
    start_time = time.time()
    arango_features_preprocessing()
    arango_edge_preprocessing()
    print("--- Dataset preprocessed in %s seconds ---" % (time.time() - start_time))
    

if __name__ == '__main__':
    preprocessing_pipeline()