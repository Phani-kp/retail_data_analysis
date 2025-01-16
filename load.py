def save_to_parquet(data, output_path):
    data.write.parquet(output_path, mode="overwrite")

if __name__ == "__main__":
    from transformation import transform_data
    from ingestion import load_data

    raw_data = load_data("data/raw/online_retail.csv")
    processed_data = transform_data(raw_data)
    save_to_parquet(processed_data, "data/processed/retail_data.parquet")
