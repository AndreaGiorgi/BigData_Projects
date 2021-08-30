from dataset_converter import loading_pipeline
from dataset_preprocessing import preprocessing_pipeline

def pipeline():
    loading_pipeline()
    preprocessing_pipeline()

if __name__ == '__main__':
    pipeline()
    