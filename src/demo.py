from data_ingestion import data_ingestion
from db_sync import db_sync
from data_quality import run_data_quality_checks

def demo():
    # data_ingestion()
    run_data_quality_checks()
    db_sync()
    


if __name__ == "__main__":
    demo()