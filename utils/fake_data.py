from faker import Faker
import pandas as pd
import numpy as np

fake = Faker(locale="pt_BR")

def generate_fake_real_estate_agents_df(num_agents: int) -> pd.DataFrame:
    fake_agent_list = []
    for i in range(num_agents):
        fake_agent_list.append(
            {
                "agent_name": fake.name(),
                "agent_cpf": fake.cpf(),
                "agent_phone": fake.cellphone_number()
            }
        )
    return pd.DataFrame(fake_agent_list)

def generate_fake_realty_sales_df(num_transactions: int, num_agents: int) -> pd.DataFrame:
    
    df_agents = generate_fake_real_estate_agents_df(num_agents=num_agents)
    
    fake_transaction_list = []
    for i in range(num_transactions):

        agent = df_agents.iloc[i % num_agents, :]

        fake_transaction_list.append(
            {
                "seller_name": fake.name(),
                "seller_cpf": fake.cpf(),
                "seller_phone": fake.cellphone_number(),
                "buyer_name": fake.name(),
                "buyer_cpf": fake.cpf(),
                "buyer_phone": fake.cellphone_number(),
                "agent_name": agent["agent_name"],
                "agent_cpf": agent["agent_cpf"],
                "agent_phone": agent["agent_phone"],
                "property_address": fake.address().replace("\n", ", "),
                "property_id": fake.ssn(),
                "property_value": np.random.randint(100, 800) * 1000,
                "total_comission_percentage": float(np.random.randint(4, 6 + 1 )) / 100,
                "agent_comission_share": float(np.random.randint(20, 80 + 1 )) / 100,
            }
        )

    return pd.DataFrame(fake_transaction_list)


# monthly_sales_df = generate_fake_realty_sales_df(300, 10)
# monthly_sales_df.to_csv('monthly_sales.csv', index=False)
