from fastapi import FastAPI
from sqlalchemy import create_engine
import pandas as pd
import os

app = FastAPI()

DATABASE_URL = os.getenv("DATABASE_URL")
engine = create_engine(DATABASE_URL)

@app.get("/ping")
def ping():
    return {"message": "API is running!"}

@app.get("/sales")
def get_sales():
    query = "SELECT * FROM sales ORDER BY timestamp DESC LIMIT 10"
    df = pd.read_sql(query, engine)
    return df.to_dict(orient="records")

@app.get("/summary")
def get_summary():
    df = pd.read_sql("SELECT * FROM sales_summary ORDER BY date DESC LIMIT 7", engine)
    return df.to_dict(orient="records")
