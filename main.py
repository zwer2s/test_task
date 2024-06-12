import os
import logging
import requests
import pandas as pd
from datetime import datetime


COMPARE_FILE_PATH = "data/product_prices_calculated.parquet"
DATA_DIR = "data"
REFERENCE_FILE_NAME = "products_new.parquet"
REFERENCE_FILE_PATH = os.path.join(DATA_DIR, REFERENCE_FILE_NAME)


def setup_logging():
    log_dir = "logs"
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    # Создание уникального имени файла для логов на основе текущей даты и времени
    log_file_name = datetime.now().strftime("log_%Y-%m-%d_%H-%M-%S.log")
    log_file_path = os.path.join(log_dir, log_file_name)

    # Настройка базовой конфигурации логирования для записи в файл
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file_path),
            logging.StreamHandler()
        ]
    )


def get_all_products(base_url="https://dummyjson.com/products", limit=30, skip=0):
    """
    Getting the products list with all details from the link
    :param base_url: (str) link where products are listed
    :param limit: (int) amount of products to get
    :param skip: (int) how many products to skip
    :return: (list) list of products with details
    """
    all_products = []

    try:
        response = requests.get(f"{base_url}?limit=1")
        response.raise_for_status()
        data = response.json()
        total_products = data['total']
        logging.info(f"Total products found: {total_products}")

        while skip < total_products:
            response = requests.get(f"{base_url}?limit={limit}&skip={skip}")
            response.raise_for_status()
            data = response.json()
            all_products.extend(data['products'])
            skip += limit
            logging.info(f"Fetched {len(data['products'])} products, skip={skip}")
    except requests.RequestException as e:
        logging.error(f"Error fetching products: {e}")
        return []

    return all_products


def extract_relevant_fields(products):
    """
    Extracting relevant fields from products to another list
    :param products: (list) list of products
    :return: (list) list of relevant fields
    """
    def calculate_final_price(product):
        return round(product['price'] - product['price'] * product['discountPercentage'] / 100, 2)

    extracted_data = [
        {
            "title": product["title"],
            "id": product["id"],
            "final_price": calculate_final_price(product)
        }
        for product in products
    ]
    logging.info(f"Extracted relevant fields for {len(products)} products")
    return extracted_data


def save_to_parquet(data, directory, file_name):
    """
    Saving data to parquet file
    :param data: (list) data to save
    :param directory: (str) path to directory where data should be saved
    :param file_name: (str) file name where data should be saved
    :return: (str) path to the file where data will be saved
    """
    if not os.path.exists(directory):
        os.makedirs(directory)
        logging.info(f"Created directory: {directory}")
    file_path = os.path.join(directory, file_name)
    df = pd.DataFrame(data)
    df.to_parquet(file_path, engine='pyarrow', index=False)
    logging.info(f"Data saved to {file_path}")
    return file_path


def find_most_expensive_product(file_path):
    """
    Finding the most expensive product
    :param file_path: (str) path to file with required data
    :return: (dict) dictionary with most expensive product
    """
    df = pd.read_parquet(file_path, engine='pyarrow')
    most_expensive_product = df.loc[df['final_price'].idxmax()]
    logging.info(
        f"Most expensive product: {most_expensive_product['title']} with price {most_expensive_product['final_price']}"
    )
    return most_expensive_product.to_dict()


def find_missing_data(reference_file_path, compare_file_path):
    """
    Comparing 2 data files and looking for missing rows in one of them
    :param reference_file_path: (str) path to reference file
    :param compare_file_path: (str) path to file for comparison
    :return: (pandas.core.frame.DataFrame) dataframe with missing data
    """
    reference_df = pd.read_parquet(reference_file_path, engine='pyarrow')
    compare_df = pd.read_parquet(compare_file_path, engine='pyarrow')
    merged_df = reference_df.merge(compare_df, on=['id', 'title'], how='left', indicator=True)
    missing_data = merged_df[merged_df['_merge'] == 'left_only'].drop(columns=['_merge'])
    logging.info(f"Found {missing_data.shape[0]} missing products")
    return missing_data


def count_matching_prices(reference_file_path, compare_file_path):
    """
    Counting how many products match in 2 files
    :param reference_file_path: (str) path to reference file
    :param compare_file_path: (str) path to file for comparison
    :return: (int) number of products match in 2 files
    """
    reference_df = pd.read_parquet(reference_file_path, engine='pyarrow')
    compare_df = pd.read_parquet(compare_file_path, engine='pyarrow')
    merged_df = reference_df.merge(compare_df, on=['id', 'title', 'final_price'], how='inner')
    matching_count = merged_df.shape[0]
    logging.info(f"Number of matching prices: {matching_count}")
    return matching_count


def execute_task(compare_file_path=COMPARE_FILE_PATH):
    """
    Function for visually displaying the results of an interview task
    :param compare_file_path: (str) path to file with expected data
    :return: None
    """
    logging.info("Starting task execution")
    products = get_all_products()
    if not products:
        logging.error("No products fetched, terminating task.")
        return

    relevant_data = extract_relevant_fields(products)
    reference_file_path = save_to_parquet(relevant_data, DATA_DIR, REFERENCE_FILE_NAME)

    print("1 - What product is the most expensive according to actual data?")
    most_expensive_product = find_most_expensive_product(reference_file_path)
    print(f"The most expensive is: {most_expensive_product['title']}\n")

    print("2 - What product is missing in expected data?")
    missing_data = find_missing_data(reference_file_path, compare_file_path)
    missing_titles = missing_data['title'].tolist()
    print("Missing items:")
    for title in missing_titles:
        print(title)
    print("\n")

    print("3 - For how many rows final price in expected data matches with calculated price from actual data?")
    matching_count = count_matching_prices(reference_file_path, compare_file_path)
    print(f"Items with similar price: {matching_count}")

    logging.info("Task execution finished")


if __name__ == "__main__":
    setup_logging()
    execute_task()
