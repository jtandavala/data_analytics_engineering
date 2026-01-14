import os
import yaml
from loguru import logger
from extract import extract_data
from load import load_data
from transform import (
    transform_customers,
    transform_geolocations,
    transform_order_items,
    transform_order_payments,
    transform_reviews,
    transform_orders,
    transform_products,
    transform_categories,
    transform_sellers,
)


current_dir = os.path.dirname(os.path.abspath(__file__))
config_path = os.path.join(current_dir, "config.yml")

with open(config_path, "r") as file:
    config_data = yaml.safe_load(file)


def run_pipeline():
    logger.info("Starting ETL pipeline")
    customer_df = extract_data(config_data["customer_filepath"])
    geoloadcation_df = extract_data(config_data["geolocation_filepath"])
    order_items_df = extract_data(config_data["order_items_filepath"])
    order_payments_df = extract_data(config_data["order_payment_filepath"])
    order_reviews_df = extract_data(config_data["order_review_filepath"])
    orders_df = extract_data(config_data["order_filepath"])
    products_df = extract_data(config_data["product_filepath"])
    categories_df = extract_data(config_data["product_category_filepath"])
    sellers_df = extract_data(config_data["seller_filepath"])

    customer_transformed_df = transform_customers(customer_df)
    geolocation_transformed_df = transform_geolocations(geoloadcation_df)
    order_items_transformed_df = transform_order_items(order_items_df)
    order_payment_transformed_df = transform_order_payments(order_payments_df)
    order_reviews_transformed_df = transform_reviews(order_reviews_df)
    orders_transformed_df = transform_orders(orders_df)
    products_transformed_df = transform_products(products_df)
    category_transformed_df = transform_categories(categories_df)
    seller_transformed_df = transform_sellers(sellers_df)

    load_data(customer_transformed_df, config_data["customer_table_PSQL"])
    load_data(geolocation_transformed_df, config_data["geolocation_table_PSQL"])
    load_data(order_items_transformed_df, config_data["order_items_table_PSQL"])
    load_data(order_payment_transformed_df, config_data["order_payments_table_PSQL"])
    load_data(order_reviews_transformed_df, config_data["order_overviews_table_PSQL"])
    load_data(orders_transformed_df, config_data["orders_table_PSQL"])
    load_data(products_transformed_df, config_data["products_table_PSQL"])
    load_data(category_transformed_df, config_data["categories_table_PSQL"])
    load_data(seller_transformed_df, config_data["sellers_table_PSQL"])


if __name__ == "__main__":
    run_pipeline()
