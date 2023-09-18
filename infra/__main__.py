"""An AWS Python Pulumi program"""

import pulumi
from pulumi_aws import s3, emr, glue

raw = s3.Bucket('raw')
trusted = s3.Bucket('trusted')
analytic = s3.Bucket('analytic')

# TODO define EMR cluster
# cluster = emr.Cluster()

# TODO define Glue crawler
# crawler = glue.Crawler()

pulumi.export('raw_bucket_name', raw.id)
pulumi.export('trusted_bucket_name', trusted.id)
pulumi.export('analytic_bucket_name', analytic.id)
# pulumi.export('emr_cluser_id', cluster.id)
