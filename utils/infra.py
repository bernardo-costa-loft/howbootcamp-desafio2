"""An AWS Python Pulumi program"""

import pulumi
from pulumi_aws import s3, emr, iam

def create_data_lake_buckets():
    raw = s3.Bucket('raw')
    trusted = s3.Bucket('trusted')
    analytic = s3.Bucket('analytic')
    pulumi.export('raw_bucket_name', raw.id)
    pulumi.export('trusted_bucket_name', trusted.id)
    pulumi.export('analytic_bucket_name', analytic.id)

def create_emr_spark_cluster():

    emr_role = iam.Role(
        "HowBootcamp_EMR_role",
        assume_role_policy="""
                            {
                            "Version": "2012-10-17",
                            "Statement": [
                                {
                                    "Effect": "Allow",
                                    "Principal": {
                                        "Service": "elasticmapreduce.amazonaws.com"
                                    },
                                    "Action": "sts:AssumeRole"
                                }
                            ]
                            }
                            """
    )

    emr_policy = iam.RolePolicyAttachment(
        "HowBootcamp_EMR_policy_attachment",
        role=emr_role.id,
        policy_arn="arn:aws:iam::aws:policy/AmazonElasticMapReduceFullAccess" # alternativa: AmazonElasticMapReduceRole
    )

    instance_profile = iam.InstanceProfile("HowBootcamp_EMR_EC2_instance_profile",
        role=emr_role.id
    )

    cluster = emr.Cluster(
        "HowBootcamp_EMR_Cluster_desafio2",
        release_label="emr-6.12.0",
        applications=['Spark'],
        service_role=emr_role.arn,
        ec2_attributes=emr.ClusterEc2AttributesArgs(
            instance_profile=instance_profile.arn
        ),
        master_instance_group=emr.ClusterMasterInstanceGroupArgs(
            instance_type="c4.large"
        ),
        core_instance_group=emr.ClusterCoreInstanceGroupArgs(
            instance_count=1,
            instance_type='c4.large'
        )
    )
    pulumi.export('emr_cluser_id', cluster.id)

def create_glue_crawler():
    pass
    # TODO define Glue crawler
    # crawler = glue.Crawler()
