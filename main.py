import requests
import pandas as pd
import os


def get_all_products(limit=30):
    base_url = "https://dummyjson.com/products"
    all_products = []
    skip = 0
    total_products = 0

    # Получаем общее количество продуктов
    response = requests.get(f"{base_url}?limit=1")
    if response.status_code == 200:
        data = response.json()
        total_products = data['total']

    # Итерируемся по страницам данных
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
    a = 1
    return [
        {
            "title": product["title"],
            "id": product["id"],
            "final_price": round(product['price'] - product['price'] * product['discountPercentage'] / 100, 2)
        }
        for product in products
    ]


def save_to_parquet(data, directory, file_name):
    if not os.path.exists(directory):
        os.makedirs(directory)
    file_path = os.path.join(directory, file_name)
    df = pd.DataFrame(data)
    df.to_parquet(file_path, engine='pyarrow', index=False)


def find_most_expensive_product(file_path):
    # Прочитать данные из файла Parquet
    df = pd.read_parquet(file_path, engine='pyarrow')

    # Найти строку с максимальной ценой
    most_expensive_product = df.loc[df['final_price'].idxmax()]

    return most_expensive_product.to_dict()


# Пример использования функции
# products = get_all_products()
# print(f"Получено {len(products)} продуктов.")
# relevant_data = extract_relevant_fields(products)
# print(relevant_data)
# save_to_parquet(relevant_data, "./data", "products_new.parquet")

file_path = "data/products_new.parquet"
most_expensive_product = find_most_expensive_product(file_path)
print(f"Самый дорогой товар: {most_expensive_product['title']}")
