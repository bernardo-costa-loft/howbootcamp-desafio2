from datetime import datetime
from io import BytesIO
import random

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

    s3_path = "comissions2"
    s3_filename = f"{s3_path}/sales_{datetime.now().strftime('%Y-%m-%d-%H%M%S')}.csv"

    s3 = s3_bucket(bucket_name)
    s3.upload(
        file=csv_buffer.getvalue(),
        s3_filename=s3_filename
    )

    print(f"Uploaded {s3_filename}")


if __name__ == "__main__":
    df = generate_data(50)
    send_df_to_s3(df, "782642054006-raw")
