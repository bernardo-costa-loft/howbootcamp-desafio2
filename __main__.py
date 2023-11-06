from utils.infra import create_data_lake_buckets, create_emr_spark_cluster

def create_infrastructure():
    result_args = create_data_lake_buckets()
    create_emr_spark_cluster(result_args)

if __name__ == "__main__":
    create_infrastructure()