import requests
import pandas as pd
import os

COMPARE_FILE_PATH = "data/product_prices_calculated.parquet"


def get_all_products(base_url="https://dummyjson.com/products", limit=30, skip=0):
    all_products = []
    total_products = 0

    # Getting the amount of items
    response = requests.get(f"{base_url}?limit=1")
    if response.status_code == 200:
        data = response.json()
        total_products = data['total']

    # Iterating through the pages of data
    while skip < total_products:
        response = requests.get(f"{base_url}?limit={limit}&skip={skip}")
        if response.status_code == 200:
            data = response.json()
            all_products.extend(data['products'])
            skip += limit
        else:
            break

    return all_products


def extract_relevant_fields(products):
    def calculate_final_price(product):
        return round(product['price'] - product['price'] * product['discountPercentage'] / 100, 2)
    return [
        {
            "title": product["title"],
            "id": product["id"],
            "final_price": calculate_final_price(product)
        }
        for product in products
    ]


def save_to_parquet(data, directory, file_name):
    if not os.path.exists(directory):
        os.makedirs(directory)
    file_path = os.path.join(directory, file_name)
    df = pd.DataFrame(data)
    df.to_parquet(file_path, engine='pyarrow', index=False)
    return file_path


def find_most_expensive_product(file_path):
    # Getting data from parquet file
    df = pd.read_parquet(file_path, engine='pyarrow')

    # Looking for an item with the biggest price
    most_expensive_product = df.loc[df['final_price'].idxmax()]

    return most_expensive_product.to_dict()


def find_missing_data(reference_file_path, compare_file_path):
    # Getting data from parquet files
    reference_df = pd.read_parquet(reference_file_path, engine='pyarrow')
    compare_df = pd.read_parquet(compare_file_path, engine='pyarrow')

    # Searching for missing items
    merged_df = reference_df.merge(compare_df, on=['id', 'title'], how='left', indicator=True)
    missing_data = merged_df[merged_df['_merge'] == 'left_only'].drop(columns=['_merge'])

    return missing_data


def count_matching_prices(reference_file_path, compare_file_path):
    # Getting data from parquet files
    reference_df = pd.read_parquet(reference_file_path, engine='pyarrow')
    compare_df = pd.read_parquet(compare_file_path, engine='pyarrow')

    # Searching for the equal data
    merged_df = reference_df.merge(compare_df, on=['id', 'title', 'final_price'], how='inner')

    # Counting rows of equal data
    matching_count = merged_df.shape[0]

    return matching_count


def execute_task(compare_file_path=COMPARE_FILE_PATH):
    # Getting all products
    products = get_all_products()
    # Getting required data from products and calculating it
    relevant_data = extract_relevant_fields(products)
    # Saving data to parquet file
    reference_file_path = save_to_parquet(relevant_data, "./data", "products_new.parquet")

    # 1 - Looking the most expensive item
    print("1 - What product is the most expensive according to actual data?")
    most_expensive_product = find_most_expensive_product(reference_file_path)
    print(f"The most expensive is: {most_expensive_product['title']}")
    print("\n")

    # 2 - Looking for missing data in expected data?
    print("2 - What product is missing in expected data?")
    missing_data = find_missing_data(reference_file_path, compare_file_path)
    missing_titles = missing_data['title'].tolist()
    print("Missing data:")
    for title in missing_titles:
        print(title)
    print("\n")

    # 3 - For how many rows final price in expected data matches with calculated price from actual data?
    print("3 - For how many rows final price in expected data matches with calculated price from actual data?")
    matching_count = count_matching_prices(reference_file_path, compare_file_path)

    print(f"Items with similar price: {matching_count}")


execute_task()
