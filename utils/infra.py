"""An AWS Python Pulumi program"""

import pulumi
from pulumi_aws import s3, emr, glue

def create_data_lake_buckets():
    raw = s3.Bucket('raw')
    trusted = s3.Bucket('trusted')
    analytic = s3.Bucket('analytic')
    pulumi.export('raw_bucket_name', raw.id)
    pulumi.export('trusted_bucket_name', trusted.id)
    pulumi.export('analytic_bucket_name', analytic.id)

def create_emr_spark_cluster():
    pass
    # TODO define EMR cluster
    # cluster = emr.Cluster()
    # pulumi.export('emr_cluser_id', cluster.id)

def create_glue_crawler():
    pass
    # TODO define Glue crawler
    # crawler = glue.Crawler()
