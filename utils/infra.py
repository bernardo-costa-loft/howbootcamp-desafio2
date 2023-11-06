"""An AWS Python Pulumi program"""

import pulumi
from pulumi_aws import s3, emr, iam, ec2
import subprocess


def get_pulumi_stack_output(stack_output_id: str):
    return(subprocess
      .run(
          ["poetry","run","pulumi","stack","output",stack_output_id], 
          capture_output=True
        ).stdout.decode(encoding="utf-8")
    )


def create_data_lake_buckets():
    raw = s3.Bucket('raw')
    trusted = s3.Bucket('trusted')
    analytic = s3.Bucket('analytic')

    raw_to_trusted = s3.BucketObject(
        "raw_to_trusted.py",
        bucket=raw.id,
        source=pulumi.FileAsset("./scripts/raw_to_trusted.py")
    )

    pulumi.export('raw_bucket_name', raw.id)
    pulumi.export('trusted_bucket_name', trusted.id)
    pulumi.export('analytic_bucket_name', analytic.id)
    pulumi.export('raw_to_trusted_script_bucket', raw_to_trusted.bucket)
    pulumi.export('raw_to_trusted_script_name', raw_to_trusted.id)

    raw_to_trusted_output = pulumi.Output.from_input(raw_to_trusted)

    return {
        "scripts": {
            "raw_to_trusted": raw_to_trusted_output
        }
    }


def create_emr_spark_cluster(args):

    raw_to_trusted = args["scripts"]["raw_to_trusted"]
    raw_to_trusted_step_command = (pulumi.Output
                                   .all(raw_to_trusted.bucket, raw_to_trusted.id)
                                   .apply(lambda x: f"s3://{x[0]}/{x[1]}")
    )
    raw_to_trusted_logs = raw_to_trusted.bucket.apply(lambda x: f's3://{x}/logs')

    keypair = ec2.KeyPair(
        "how_bootcamp_emr_keypair",
        key_name="how_bootcamp_emr_public_key",
        public_key="ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIKzY6Fo++0wMrMsL6vZBFyoTu4dQwynbVBE9tQ3YxPAh bemc@poli.ufrj.br"
    )
    
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

    emr_ec2_role = iam.Role(
        "HowBootcamp_EMR_EC2_role",
        assume_role_policy="""
                            {
                            "Version": "2012-10-17",
                            "Statement": [
                                {
                                    "Effect": "Allow",
                                    "Principal": {
                                        "Service": "ec2.amazonaws.com"
                                    },
                                    "Action": "sts:AssumeRole"
                                }
                            ]
                            }
                            """
    )

    emr_policy = iam.Policy(
        "howbootcamp_EMR_policy",
        policy="""{
                    "Version": "2012-10-17",
                    "Statement": [
                        {
                            "Sid": "VisualEditor0",
                            "Effect": "Allow",
                            "Action": "iam:CreateServiceLinkedRole",
                            "Resource": "*",
                            "Condition": {
                                "StringLike": {
                                    "iam:AWSServiceName": [
                                        "elasticmapreduce.amazonaws.com",
                                        "elasticmapreduce.amazonaws.com.cn"
                                    ]
                                }
                            }
                        },
                        {
                            "Sid": "VisualEditor1",
                            "Effect": "Allow",
                            "Action": [
                                "iam:GetPolicyVersion",
                                "kms:List*",
                                "iam:PassRole",
                                "cloudformation:DescribeStackEvents",
                                "sdb:*",
                                "iam:GetPolicy",
                                "s3:*",
                                "iam:ListRoles",
                                "cloudwatch:*",
                                "cloudformation:CreateStack",
                                "ec2:*",
                                "elasticmapreduce:*",
                                "sts:AssumeRole",
                                "sts:TagSession"
                            ],
                            "Resource": "*"
                        }
                    ]
                }
        """
    )

    emr_policy_att = iam.RolePolicyAttachment(
        "HowBootcamp_EMR_policy_attachment",
        role=emr_role.id,
        policy_arn=emr_policy.arn
    )

    emr_ec2_policy_att = iam.RolePolicyAttachment(
        "HowBootcamp_EMR_EC2_policy_attachment",
        role=emr_ec2_role.id,
        policy_arn=emr_policy.arn
    )

    instance_profile = iam.InstanceProfile("HowBootcamp_EMR_EC2_instance_profile",
        role=emr_ec2_role.id
    )

    sec_grp = ec2.SecurityGroup(
        "emr-sec-grp",
        description="Enable SSH access",
        ingress=[
            ec2.SecurityGroupIngressArgs(
                protocol="tcp",
                from_port=22,
                to_port=22,
                cidr_blocks=["0.0.0.0/0"]
            )
        ]
    )
    
    raw_to_trusted_step = emr.ClusterStepArgs(
        name="raw_to_trusted",
        action_on_failure="CONTINUE",
        hadoop_jar_step=emr.ClusterStepHadoopJarStepArgs(
            jar="command-runner.jar",
            args=["sudo","-E","spark-submit","--deploy-mode","client","--master","yarn",raw_to_trusted_step_command],
        ),
    )
    
    cluster = emr.Cluster(
        "HowBootcamp_EMR_Cluster_desafio2",
        release_label="emr-6.14.0",
        applications=["Spark","Hadoop"],
        log_uri=raw_to_trusted_logs,
        service_role=emr_role.arn,
        ec2_attributes=emr.ClusterEc2AttributesArgs(
            instance_profile=instance_profile.arn,
            key_name=keypair.key_name,
            additional_master_security_groups=sec_grp.id,
            additional_slave_security_groups=sec_grp.id
        ),
        master_instance_group=emr.ClusterMasterInstanceGroupArgs(
            instance_type="r7g.xlarge",
        ),
        core_instance_group=emr.ClusterCoreInstanceGroupArgs(
            instance_count=1,
            instance_type='r7g.xlarge'
        ),
        steps=[raw_to_trusted_step],
        auto_termination_policy=emr.ClusterAutoTerminationPolicyArgs(
            idle_timeout=180
        )
    )

    pulumi.export('emr_cluser_id', cluster.id)


def create_glue_crawler():
    pass
    # TODO define Glue crawler
    # crawler = glue.Crawler()
