def transform_data(data):
    transformed = data.na.drop(subset=["CustomerID"])
    transformed = transformed.withColumnRenamed("InvoiceNo", "Invoice_Number")
    transformed = transformed.withColumn("Revenue", data["Quantity"] * data["UnitPrice"])
    return transformed

if __name__ == "__main__":
    from ingestion import load_data

    raw_data = load_data("data/raw/online_retail.csv")
    processed_data = transform_data(raw_data)
    processed_data.show()
