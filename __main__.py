from utils.infra import create_data_lake_buckets, create_emr_spark_cluster

def create_infrastructure():
    create_data_lake_buckets()
    create_emr_spark_cluster()

if __name__ == "__main__":
    create_infrastructure()