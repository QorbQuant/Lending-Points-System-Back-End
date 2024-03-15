# This is a sample Python script.
from dotenv import load_dotenv
import os
from contracts import CONTRACT_ADDRESSES
import pandas as pd
import requests

# Main workflow
load_dotenv()  # Loads the environment variables from .env file
api_key = os.getenv('LINEA_API_KEY')
address = CONTRACT_ADDRESSES["deposit_contract"]

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


all_transactions = fetch_transactions(address, api_key)

if all_transactions:
    # Convert to DataFrame and process
    df = pd.DataFrame(all_transactions)
    if 'timeStamp' in df.columns:
        df['timeStamp'] = pd.to_datetime(df['timeStamp'], unit='s')
        df = df.sort_values(by='timeStamp', ascending=False)

    # Group, sort, and calculate points
    grouped_data = df.groupby('from')['hash'].count().reset_index(name='hash_count')
    grouped_data_sorted = grouped_data.sort_values(by='hash_count', ascending=False)
    grouped_data_sorted['points'] = grouped_data_sorted['hash_count'] * 10

    # Display the final DataFrame
    print(grouped_data_sorted)
else:
    print("No transactions found.")

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    print(grouped_data_sorted)
