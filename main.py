from dotenv import load_dotenv
import os
import psycopg2
from contracts import CONTRACT_ADDRESSES
import pandas as pd
import requests
import time

load_dotenv()
api_key = os.getenv('LINEA_API_KEY')
address = CONTRACT_ADDRESSES["deposit_contract"]
db_conn_str = os.getenv('db_conn_str')


def fetch_transactions(address, api_key):
    """fetch transactions utilizing linea API"""

    base_url = "https://api.lineascan.build/api"
    params = {
        'module': 'account',
        'action': 'txlist',
        'address': address,
        'startblock': 0,
        'endblock': 'latest',
        'page': 1,
        'offset': 100,
        'sort': 'asc',
        'apikey': api_key
    }

    transactions = []

    while True:
        response = requests.get(base_url, params=params)
        if response.status_code == 200:
            data = response.json()
            results = data.get('result', [])
            if not results:
                break
            transactions.extend(results)
            params['page'] += 1
        else:
            print("Failed to fetch data from the API. Status code:", response.status_code)
            break

    return transactions


def insert_transactions_to_db(grouped_data_sorted, db_conn_str):
    """connect to db and insert data"""

    conn = psycopg2.connect(db_conn_str)
    cur = conn.cursor()

    # Clear the table before inserting new data
    try:
        cur.execute("DELETE FROM transaction_counts")
        conn.commit()
    except Exception as e:
        print("An error occurred while trying to delete data: ", e)
        conn.rollback()  # Rollback the transaction on error

    # Now, insert the new data
    try:
        for _, row in grouped_data_sorted.iterrows():
            cur.execute(
                "INSERT INTO transaction_counts (address, hash_count, points) VALUES (%s, %s, %s)",
                (row['from'], row['hash_count'], row['points'])
            )
        conn.commit()  # Commit the transaction
    except Exception as e:
        print("An error occurred while trying to insert data: ", e)
        conn.rollback()  # Rollback the transaction on error

    cur.close()
    conn.close()


def run_hourly_task():
    """ process data and insert into db every hour """

    while True:
        all_transactions = fetch_transactions(address, api_key)

        if all_transactions:
            # Convert to DataFrame and process
            df = pd.DataFrame(all_transactions)

            # Group, sort, and calculate points
            grouped_data = df.groupby('from')['hash'].count().reset_index(name='hash_count')
            grouped_data_sorted = grouped_data.sort_values(by='hash_count', ascending=False)
            grouped_data_sorted['points'] = grouped_data_sorted['hash_count'] * 10

            # Insert into the database
            insert_transactions_to_db(grouped_data_sorted, db_conn_str)
            print("transactions inserted into db")
        else:
            print("No transactions found.")

        print("Task completed, waiting for one hour.")
        time.sleep(3600)  # Sleep for one hour


if __name__ == '__main__':
    run_hourly_task()


