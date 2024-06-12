import requests
import pandas as pd


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


# Пример использования функции
products = get_all_products()
print(f"Получено {len(products)} продуктов.")
relevant_data = extract_relevant_fields(products)
print(relevant_data)
