import os
import pandas as pd
from unittest.mock import patch, MagicMock
from main import (
    get_all_products,
    extract_relevant_fields,
    save_to_parquet,
    find_most_expensive_product,
    find_missing_data,
    count_matching_prices,
)


PRODUCTS_LIST = [
            {'id': 1, 'title': 'Product 1', 'price': 100, 'discountPercentage': 10},
            {'id': 2, 'title': 'Product 2', 'price': 200, 'discountPercentage': 20},
            {'id': 3, 'title': 'Product 3', 'price': 300, 'discountPercentage': 30},
        ]
DATA_FOR_PARQUET = [
        {'title': 'Product 1', 'id': 1, 'final_price': 90.0},
        {'title': 'Product 2', 'id': 2, 'final_price': 160.0},
        {'title': 'Product 3', 'id': 3, 'final_price': 210.0},
    ]
DATA_FOR_PARQUET_MISSING_ITEM = [
        {'title': 'Product 1', 'id': 1, 'final_price': 90.0},
        {'title': 'Product 2', 'id': 2, 'final_price': 160.0},
    ]


@patch('main.requests.get')
def test_get_all_products(mock_get):
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {
        'total': 3,
        'products': PRODUCTS_LIST
    }
    mock_get.return_value = mock_response

    products = get_all_products()
    assert len(products) == 3
    assert products[0]['title'] == 'Product 1'
    assert products[1]['price'] == 200
    assert products[2]['discountPercentage'] == 30


def test_extract_relevant_fields():
    products = PRODUCTS_LIST
    relevant_data = extract_relevant_fields(products)
    assert len(relevant_data) == 3
    assert relevant_data[0]['final_price'] == 90.0
    assert relevant_data[1]['final_price'] == 160.0
    assert relevant_data[2]['final_price'] == 210.0


def test_save_to_parquet(tmpdir):
    data = DATA_FOR_PARQUET
    directory = tmpdir.mkdir("data")
    file_name = "test.parquet"
    file_path = save_to_parquet(data, directory, file_name)

    assert os.path.exists(file_path)
    df = pd.read_parquet(file_path, engine='pyarrow')
    assert df.shape[0] == 3
    assert df.iloc[0]['final_price'] == 90.0


def test_find_most_expensive_product(tmpdir):
    data = DATA_FOR_PARQUET
    directory = tmpdir.mkdir("data")
    file_name = "test.parquet"
    file_path = save_to_parquet(data, directory, file_name)

    most_expensive_product = find_most_expensive_product(file_path)
    assert most_expensive_product['title'] == 'Product 3'
    assert most_expensive_product['final_price'] == 210.0


def test_find_missing_data(tmpdir):
    reference_data = DATA_FOR_PARQUET
    compare_data = DATA_FOR_PARQUET_MISSING_ITEM
    directory = tmpdir.mkdir("data")
    reference_file = save_to_parquet(reference_data, directory, "reference.parquet")
    compare_file = save_to_parquet(compare_data, directory, "compare.parquet")

    missing_data = find_missing_data(reference_file, compare_file)
    assert missing_data.shape[0] == 1
    assert missing_data.iloc[0]['title'] == 'Product 3'


def test_count_matching_prices(tmpdir):
    reference_data = DATA_FOR_PARQUET
    compare_data = DATA_FOR_PARQUET
    directory = tmpdir.mkdir("data")
    reference_file = save_to_parquet(reference_data, directory, "reference.parquet")
    compare_file = save_to_parquet(compare_data, directory, "compare.parquet")

    matching_count = count_matching_prices(reference_file, compare_file)
    assert matching_count == 3
