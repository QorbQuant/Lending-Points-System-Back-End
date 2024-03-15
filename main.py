from dotenv import load_dotenv
import os
import psycopg2
from contracts import CONTRACT_ADDRESSES
import pandas as pd
import requests

# Main workflow
load_dotenv()  # Loads the environment variables from .env file
api_key = os.getenv('LINEA_API_KEY')
address = CONTRACT_ADDRESSES["deposit_contract"]
db_conn_str = os.getenv('db_conn_str')  # Assuming you have your database URL in the .env file


def fetch_transactions(address, api_key):
    base_url = "https://api.lineascan.build/api"
    params = {
        'module': 'account',
        'action': 'txlist',
        'address': address,
        'startblock': 0,
        'endblock': 'latest',
        'page': 1,
        'offset': 100,  # Adjust based on API limits
        'sort': 'asc',
        'apikey': api_key
    }

    transactions = []

    while True:
        response = requests.get(base_url, params=params)
        if response.status_code == 200:
            data = response.json()
            results = data.get('result', [])
            if not results:  # No more results
                break
            transactions.extend(results)
            params['page'] += 1  # Go to the next page
        else:
            print("Failed to fetch data from the API. Status code:", response.status_code)
            break

    return transactions


def insert_transactions_to_db(grouped_data_sorted, db_conn_str):
    # Connect to your database
    conn = psycopg2.connect(db_conn_str)
    cur = conn.cursor()

    # Assuming you have a table created like this:
    # CREATE TABLE transaction_counts (address VARCHAR(255), hash_count INT, points INT);

    for _, row in grouped_data_sorted.iterrows():
        cur.execute(
            "INSERT INTO transaction_counts (address, hash_count, points) VALUES (%s, %s, %s)",
            (row['from'], row['hash_count'], row['points'])
        )

    conn.commit()  # Commit the transaction
    cur.close()
    conn.close()


all_transactions = fetch_transactions(address, api_key)

if all_transactions:
    # Convert to DataFrame and process
    df = pd.DataFrame(all_transactions)

    # Group, sort, and calculate points
    grouped_data = df.groupby('from')['hash'].count().reset_index(name='hash_count')
    grouped_data_sorted = grouped_data.sort_values(by='hash_count', ascending=False)
    grouped_data_sorted['points'] = grouped_data_sorted['hash_count'] * 10

    # Display the final DataFrame
    insert_transactions_to_db(grouped_data_sorted, db_conn_str)
else:
    print("No transactions found.")

# if __name__ == '__main__':
#     print(grouped_data_sorted)
