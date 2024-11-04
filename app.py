from flask import Flask, jsonify, render_template, send_file, make_response
from sqlalchemy import create_engine, text
from datetime import date, time, datetime
import pandas as pd
import os
import csv

app = Flask(__name__)

# Create a connection to the PostgreSQL database
engine = create_engine('postgresql://postgres:postgresp@localhost:5432/sales_inventory_db')

# Helper function to convert row to a dictionary and handle non-serializable objects
def row_to_dict(row):
    row_dict = dict(row._mapping)
    for key, value in row_dict.items():
        if isinstance(value, (date, time, datetime)):
            row_dict[key] = value.isoformat()
    return row_dict

# Route to display the main dashboard with links to all data tables
@app.route('/')
def index():
    return render_template('index.html')

# Generic function to fetch data from a table
def fetch_data(table_name):
    with engine.connect() as connection:
        query = text(f"SELECT * FROM {table_name}")
        rows = connection.execute(query)
        data = [row_to_dict(row) for row in rows]
    return data

# Route to display data in table format on the webpage
@app.route('/view/<table_name>')
def view_table(table_name):
    data = fetch_data(table_name)
    return render_template('table_view.html', data=data, table_name=table_name)

# Route to download CSV of data from any table
@app.route('/download/<table_name>')
def download_csv(table_name):
    query = f"SELECT * FROM {table_name}"
    df = pd.read_sql(query, engine)
    csv_path = f"{table_name}_data.csv"
    df.to_csv(csv_path, index=False)
    return send_file(csv_path, as_attachment=True)

# View and download for products, sales, inventory
@app.route('/products', methods=['GET'])
def get_products():
    return view_table('products')

@app.route('/sales', methods=['GET'])
def get_sales():
    return view_table('sales')

@app.route('/inventory', methods=['GET'])
def get_inventory():
    return view_table('inventory')

# Route for Sales Forecast
@app.route('/forecast_sales', methods=['GET'])
def get_forecast_sales():
    with engine.connect() as connection:
        query = text("SELECT forecast_month, forecast_sales_next_month FROM forecast_sales")
        rows = connection.execute(query)
        data = [row_to_dict(row) for row in rows]
    
    return render_template('table_view.html', data=data, table_name='Sales Forecast')

# Route for Inventory Forecast
@app.route('/forecast_inventory', methods=['GET'])
def get_forecast_inventory():
    with engine.connect() as connection:
        query = text("SELECT forecast_month, predicted_inventory FROM forecast_inventory")
        rows = connection.execute(query)
        data = [row_to_dict(row) for row in rows]
    
    return render_template('table_view.html', data=data, table_name='Inventory Forecast')

# CSV Download for Sales Forecast
@app.route('/download_forecast_sales_csv')
def download_forecast_sales_csv():
    with engine.connect() as connection:
        query = text("SELECT forecast_month, forecast_sales_next_month FROM forecast_sales")
        rows = connection.execute(query)
        data = [row_to_dict(row) for row in rows]

    csv_output = make_response()
    writer = csv.DictWriter(csv_output, fieldnames=["forecast_month", "forecast_sales_next_month"])
    writer.writeheader()
    for row in data:
        writer.writerow(row)

    csv_output.headers["Content-Disposition"] = "attachment; filename=forecast_sales.csv"
    csv_output.headers["Content-type"] = "text/csv"
    
    return csv_output

# CSV Download for Inventory Forecast
@app.route('/download_forecast_inventory_csv')
def download_forecast_inventory_csv():
    with engine.connect() as connection:
        query = text("SELECT forecast_month, predicted_inventory FROM forecast_inventory")
        rows = connection.execute(query)
        data = [row_to_dict(row) for row in rows]

    csv_output = make_response()
    writer = csv.DictWriter(csv_output, fieldnames=["forecast_month", "predicted_inventory"])
    writer.writeheader()
    for row in data:
        writer.writerow(row)

    csv_output.headers["Content-Disposition"] = "attachment; filename=forecast_inventory.csv"
    csv_output.headers["Content-type"] = "text/csv"
    
    return csv_output

if __name__ == '__main__':
    app.run(debug=True)
