import requests


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


# Пример использования функции
products = get_all_products()
print(f"Получено {len(products)} продуктов.")
print(products)

