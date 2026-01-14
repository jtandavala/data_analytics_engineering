import pandas as pd
from typing import Optional, List, Dict


def extract_data(
    filepath: str,
    select_cols: Optional[List[str]] = None,
    rename_cols: Optional[Dict[str, str]] = None,
) -> pd.DataFrame:
    try:
        df = pd.read_csv(filepath)

        if select_cols:
            df = df[select_cols]

        if rename_cols:
            df = df.rename(columns=rename_cols)

        return df
    except FileNotFoundError as e:
        print(f"Error: File not found - {e}")
    except Exception as e:
        print(f"Error: {e}")
