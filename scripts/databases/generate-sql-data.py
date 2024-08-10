import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import pyodbc

# TRANSACTION DATA
def generate_transaction_data(num_records):
    np.random.seed(0)
    data = {
        'TransactionID': np.arange(1, num_records + 1),
        'CustomerID': np.random.randint(1, 30001, num_records),
        'ProductID': np.random.randint(1, 1001, num_records),
        'Quantity': np.random.randint(1, 10, num_records),
        'TransactionDate': pd.to_datetime('2023-01-01') + pd.to_timedelta(np.random.randint(0, 365, num_records), unit='d'),
        'TransactionAmount': np.round(np.random.uniform(10, 1000, num_records), 2)
    }
    return pd.DataFrame(data)

# CRM DATA
def generate_crm_data(num_records):
    np.random.seed(1)
    data = {
        'CustomerID': np.arange(1, num_records + 1),
        'CustomerName': ['Customer_' + str(i) for i in range(1, num_records + 1)],
        'Email': ['customer' + str(i) + '@example.com' for i in range(1, num_records + 1)],
        'Phone': ['123-456-7890'] * num_records,
        'SignupDate': pd.to_datetime('2022-01-01') + pd.to_timedelta(np.random.randint(0, 730, num_records), unit='d')
    }
    return pd.DataFrame(data)

# MARKETING ATTRIBUTION DATA
def generate_marketing_attribution_data(num_records):
    np.random.seed(2)
    data = {
        'AttributionID': np.arange(1, num_records + 1),
        'CustomerID': np.random.randint(1, 30001, num_records),
        'CampaignID': np.random.randint(1, 101, num_records),
        'AttributionDate': pd.to_datetime('2023-01-01') + pd.to_timedelta(np.random.randint(0, 365, num_records), unit='d'),
        'AttributionValue': np.round(np.random.uniform(5, 500, num_records), 2)
    }
    return pd.DataFrame(data)

# SAVE TO SQL SERVER
def save_to_sql_server(df, table_name, connection_string):
    try:
        engine = create_engine(connection_string)
        df.to_sql(table_name, engine, if_exists='replace', index=False)
        print(f"Data successfully saved to {table_name} table.")
    except Exception as e:
        print(f"Error saving data to {table_name} table: {e}")

# Main script
def main():
    transaction_data = generate_transaction_data(50000)
    crm_data = generate_crm_data(30000)
    marketing_attribution_data = generate_marketing_attribution_data(20000)

    connection_string = "your_string"

    save_to_sql_server(transaction_data, 'crm', connection_string)
    save_to_sql_server(crm_data, 'transactions', connection_string)
    save_to_sql_server(marketing_attribution_data, 'attribution', connection_string)

if __name__ == "__main__":
    main()
