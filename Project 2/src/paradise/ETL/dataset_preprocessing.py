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

def nodes_preprocessing():

    dataset_address = pd.read_csv("data/paradise/paradise_papers.nodes.address.csv")
    dataset_entity = pd.read_csv("data/paradise/paradise_papers.nodes.entity.csv")
    dataset_intermediary = pd.read_csv("data/paradise/paradise_papers.nodes.intermediary.csv")
    dataset_officer = pd.read_csv("data/paradise/paradise_papers.nodes.officer.csv")
    dataset_other = pd.read_csv("data/paradise/paradise_papers.nodes.other.csv")

    print(dataset_address.head())
    print(dataset_entity.head())
    print(dataset_intermediary.head())
    print(dataset_officer.head())
    print(dataset_other.head())

def preprocessing_pipeline():
    
    start_time = time.time()
    nodes_preprocessing()
    print("--- Dataset preprocessed in %s seconds ---" % (time.time() - start_time))
    

if __name__ == '__main__':
    preprocessing_pipeline()