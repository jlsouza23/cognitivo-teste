from boto3 import client
from datetime import datetime
import json

emr = client('emr', region_name='sa-east-1')

def lambda_handler(event, context):
    # TODO implement
    response = emr.run_job_flow(
        Name='Cluster - Datalake - {}'.format(
            datetime.now().strftime('%Y-%m-%d %H:%M:%S')),
        LogUri='s3://s3_cognitivo/emr/cluster/',
        ReleaseLabel='emr-5.29.0',
        VisibleToAllUsers=True,
        Instances={
            'KeepJobFlowAliveWhenNoSteps': False,
            'TerminationProtected': False,
            'InstanceGroups': [
                {"InstanceCount":1,"Market": "SPOT", "InstanceRole":"CORE","InstanceType":"m5.xlarge","Name":"Core"},
                {"InstanceCount":1,"Market": "SPOT", "InstanceRole":"MASTER","InstanceType":"m5.xlarge","Name":"Master"}
            ],
            'Ec2SubnetId': 'subnet-3ed87b59',
            'EmrManagedSlaveSecurityGroup': 'sg-0992a70404368f6b4',
            'EmrManagedMasterSecurityGroup': 'sg-0fa96243505e4548a'
        },
        StepConcurrencyLevel = 2,
        Applications=[
            {
                'Name': 'Hadoop'
            },
            {
                'Name': 'Spark'
            },
            {
                'Name': 'Livy'
            },
            {
                'Name': 'Hive'
            }
        ],
        ServiceRole='EMR_DefaultRole',
        JobFlowRole='EMR_EC2_DefaultRole',
        Steps=[{
                'Name': 'teste',
                'ActionOnFailure': 'CONTINUE',
                'HadoopJarStep': {
                    'Jar': 'command-runner.jar',
                    'Args': ["spark-submit", "ss3://s3_cognitivo/script/job-spark.py"]
                }}
        ],
        Configurations=[
            {
                "Classification": "hive-site",
                "Properties": {
                    "hive.metastore.client.factory.class": "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory"
                }
            },
            {
                "Classification": "spark-hive-site",
                "Properties": {
                    "hive.metastore.client.factory.class": "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory"
                }
            },
            {
                "Classification": "spark-env",
                "Configurations": [
                {
                    "Classification": "export",
                    "Properties": {
                        "PYSPARK_PYTHON": "/usr/bin/python3"
                    }
                }
                ]
            }
        ],
        Tags=[
                {
                    'Key': 'business_owner',
                    'Value': 'data-flow'
                }
            ]
    )

    print(response)

    return {
        "statusCode": 200,
        "body": json.dumps(response['JobFlowId'])
    }

if __name__ == '__main__':
    ex = lambda_handler({}, {})
    print(ex)
