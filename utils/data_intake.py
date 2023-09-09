import boto3
import os
from dotenv import load_dotenv

load_dotenv()

class s3_bucket:

    def __init__(self, bucket_name):
        self.bucket_name = bucket_name
        self.resource = self._get_s3_access()


    def _get_s3_access(self):

        session = boto3.Session(
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY")
        )

        resource = session.resource(
            service_name="s3",
            region_name="us-east-1"
        )

        return resource
    
    
    def upload(self, file, s3_filename):

        (
            self
            .resource
            .Object(self.bucket_name, s3_filename)
            .put(Body=file)
        )
        