from datetime import datetime
import pandas as pd


def transform_customers(customers: pd.DataFrame) -> pd.DataFrame:
    """add transformation logic here"""
    customers["loaded_at"] = datetime.now()
    return customers


def transform_geolocations(geolocations: pd.DataFrame) -> pd.DataFrame:
    """add transformation logic here"""
    geolocations["loaded_at"] = datetime.now()
    return geolocations


def transform_order_items(order_items: pd.DataFrame) -> pd.DataFrame:
    """add transformation logic here"""
    order_items["loaded_at"] = datetime.now()
    return order_items


def transform_order_payments(payments: pd.DataFrame) -> pd.DataFrame:
    """add transformation logic here"""
    payments["loaded_at"] = datetime.now()
    return payments


def transform_reviews(reviews: pd.DataFrame) -> pd.DataFrame:
    """add transformation logic here"""
    reviews["loaded_at"] = datetime.now()
    return reviews


def transform_orders(orders: pd.DataFrame) -> pd.DataFrame:
    """add transformation logic here"""
    orders["loaded_at"] = datetime.now()
    return orders


def transform_products(products: pd.DataFrame) -> pd.DataFrame:
    """add transformation logic here"""
    products["loaded_at"] = datetime.now()
    return products


def transform_categories(categories: pd.DataFrame) -> pd.DataFrame:
    """add transformation logic here"""
    categories["loaded_at"] = datetime.now()
    return categories


def transform_sellers(sellers: pd.DataFrame) -> pd.DataFrame:
    """add transformation logic here"""
    sellers["loaded_at"] = datetime.now()
    return sellers
