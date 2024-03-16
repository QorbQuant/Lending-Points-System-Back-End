import pandas as pd


def process_data(transactions):
    """Process transactions and return grouped and sorted data with points."""

    df = pd.DataFrame(transactions)

    # Group, sort, and calculate points
    grouped_data = df.groupby('from')['hash'].count().reset_index(name='hash_count')
    grouped_data_sorted = grouped_data.sort_values(by='hash_count', ascending=False)
    grouped_data_sorted['points'] = grouped_data_sorted['hash_count'] * 10

    return grouped_data_sorted


def process_data_dicts(transactions):
    return process_data(transactions).to_dict("records")