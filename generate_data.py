from datetime import datetime
from io import BytesIO
import random
import subprocess

from utils.fake_data import generate_fake_realty_sales_df
from utils.data_intake import s3_bucket


def generate_data(max_rows):
    num_transactions = random.randint(1,max_rows)
    num_agents = random.randint(1,num_transactions)

    df = generate_fake_realty_sales_df(
        num_transactions,
        num_agents
    )

    print(f"Generated {num_transactions} transactions with {num_agents} agents")
    
    return df


def send_df_to_s3(df, bucket_name):
    csv_buffer = BytesIO()
    df.to_csv(csv_buffer)

    s3_path = "transactions"
    s3_filename = f"{s3_path}/{datetime.now().strftime('%Y-%m-%d-%H%M%S')}.csv"

    s3 = s3_bucket(bucket_name)
    s3.upload(
        file=csv_buffer.getvalue(),
        s3_filename=s3_filename
    )

    print(f"Uploaded {s3_filename}")


def get_pulumi_stack_output(stack_output_id: str):
    return(subprocess
      .run(
          ["poetry","run","pulumi","stack","output",stack_output_id], 
          capture_output=True
        ).stdout.decode(encoding="utf-8")
    )


if __name__ == "__main__":
    df = generate_data(50)
    bucket_name = get_pulumi_stack_output("raw_bucket_name")
    send_df_to_s3(df, bucket_name)
