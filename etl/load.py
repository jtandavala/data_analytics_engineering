import yaml
import os
import pandas as pd
from sqlalchemy import create_engine, text
from loguru import logger

current_dir = os.path.dirname(os.path.abspath(__file__))
config_path = os.path.join(current_dir, "config.yml")

with open(config_path, "r") as file:
    config_data = yaml.safe_load(file)


def load_data(df: pd.DataFrame, table_name: str):
    connection_string = (
        f"postgresql://{config_data['postgresql']['user']}:"
        f"{config_data['postgresql']['password']}@"
        f"{config_data['postgresql']['host']}:"
        f"{config_data['postgresql']['port']}/"
        f"{config_data['postgresql']['database']}"
    )

    engine = create_engine(connection_string)

    with engine.connect() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS olist_raw;"))
        conn.commit()

    df.to_sql(
        name=table_name.split(".")[-1],
        schema=table_name.split(".")[0],
        con=engine,
        if_exists="replace",
        index=False,
        method="multi",
    )
    engine.dispose()

    logger.info(f"Data loaded successfully to {table_name}")
