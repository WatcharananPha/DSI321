import pandas as pd

def validate_data(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
        
    required_columns = ['scrape_datetime_utc', 'current_value_mw', 'data_source_url']
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        raise ValueError(f"Missing required columns: {missing_columns}")

    df = df.dropna(subset=['current_value_mw'])
    df = df[df['current_value_mw'] > 0]
    
    return df