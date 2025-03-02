import os
import sqlite3
import pandas as pd
from langchain_community.utilities import SQLDatabase


os.environ["GROQ_API_KEY"] = "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"


def create_tables(cursor):
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS gumanitarka (

        row_id INTEGER PRIMARY KEY AUTOINCREMENT,   -- Auto-incrementing primary key
        order_id VARCHAR(255) NOT NULL,                      -- Unique identifier for each order
        order_date DATE NOT NULL,                    -- Order date (can be stored as TEXT for flexibility)
        ship_date DATE NOT NULL,                     -- Ship date (stored as TEXT for flexibility)
        ship_mode VARCHAR(255),                              -- Mode of shipping (e.g., Second Class, Standard Class)

        customer_id VARCHAR(255) NOT NULL,                   -- Customer identifier
        customer_name VARCHAR(255) NOT NULL,                 -- Customer's full name
        segment VARCHAR(255),                                -- Customer segment (e.g., Consumer, Corporate)
        country VARCHAR(255),                                -- Country of the customer
        city VARCHAR(255),                                   -- City of the customer
        state VARCHAR(255),                                  -- State of the customer
        postal_code VARCHAR(255),                            -- Postal code of the customer

        region VARCHAR(255),                                 -- Region of the customer
        product_id VARCHAR(255) NOT NULL,                    -- Unique product identifier
        category VARCHAR(255),                               -- Product category (e.g., Furniture, Office Supplies)
        sub_category VARCHAR(255),                           -- Sub-category of the product (e.g., Bookcases, Chairs)
        product_name TEXT NOT NULL,                  -- Name of the product

        sales REAL NOT NULL,                         -- Total sales amount (numeric)
        quantity INTEGER NOT NULL,                   -- Quantity of products sold
        discount REAL,                               -- Discount applied to the order
        profit REAL                                  -- Profit made from the sale
    );''') # 21


def insert_gumanitarka_to_db(cursor, path_to_db="gumanitarka.xls"):
    df = pd.read_excel(path_to_db)
    df['Order Date'] = pd.to_datetime(df['Order Date']).dt.strftime('%Y-%m-%d')
    df['Ship Date'] = pd.to_datetime(df['Ship Date']).dt.strftime('%Y-%m-%d')

    for index, row in df.iterrows(): # 20
        cursor.execute('''
            INSERT INTO gumanitarka (
                order_id, order_date, ship_date, ship_mode,
                customer_id, customer_name, segment, country, city, state, postal_code,
                region, product_id, category, sub_category, product_name,
                sales, quantity, discount, profit
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            row['Order ID'], row['Order Date'], row['Ship Date'], row['Ship Mode'], row['Customer ID'],
            row['Customer Name'], row['Segment'], row['Country'], row['City'], row['State'], row['Postal Code'],
            row['Region'], row['Product ID'], row['Category'], row['Sub-Category'], row['Product Name'],
            row['Sales'], row['Quantity'], row['Discount'], row['Profit']
        ))


def run_code():
    try:
        conn = sqlite3.connect("gumanitarka.db")
        cursor = conn.cursor()
        create_tables(cursor)
        conn.commit()
        insert_gumanitarka_to_db(cursor, path_to_db="gumanitarka.xls")
        conn.commit()
    except Exception as e:
        print(f"Exception:: {e}")
    finally:
        conn.close()


def validate_insertion(path_to_db="gumanitarka.db", db_uri="sqlite:///gumanitarka.db"):
    conn = sqlite3.connect(path_to_db)
    cursor = conn.cursor()

    try:
        cursor.execute('''SELECT * FROM gumanitarka LIMIT 5''')
        rows = cursor.fetchall()
        for row in rows:
            print(f"Row from db:: {row}")


        db = SQLDatabase.from_uri(db_uri)
        print(db.dialect)
        print(db.get_usable_table_names())
        rows = db.run("SELECT * FROM gumanitarka LIMIT 5;")
        for row in rows:
            print(f"Row from langchain db:: {row}")
    except Exception as e:
        print(f"Exception:: {e}")
    finally:
        conn.close()