import boto3
import random
from utils.fake_data import generate_fake_realty_sales_df


if __name__ == "__main__":

    num_transactions = random.randint(1,10)
    num_agents = random.randint(1,num_transactions)

    df = generate_fake_realty_sales_df(
        num_transactions,
        num_agents
    )

    print(df.info())

