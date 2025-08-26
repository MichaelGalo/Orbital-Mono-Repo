from data_ingestion import data_ingestion
from db_sync import db_sync
from db_sync import db_sync_alt

def demo():
    data_ingestion()
    # db_sync()


if __name__ == "__main__":
    demo()