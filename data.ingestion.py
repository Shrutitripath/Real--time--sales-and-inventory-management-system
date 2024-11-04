import pandas as pd
from sqlalchemy import create_engine, text
from data_cleaning import clean_and_validate_data
import os
import data_forecasting
# this part of our project which is commented out is for sending alerts but we couldn't finish it in the time crunch, we did all the rest improvements and corrections except this
# import smtplib
# from email.mime.multipart import MIMEMultipart
# from email.mime.text import MIMEText

# # Email configuration - Update with your details
# SMTP_SERVER = 'smtp.gmail.com'  # For Gmail SMTP server
# SMTP_PORT = 587  # Standard port for email
# SENDER_EMAIL = 'project0.manager0@gmail.com'
# SENDER_PASSWORD = 'prgo azvr ugfo rsch' # write you app password from gmail here
# RECIPIENT_EMAIL = 'engr.akshdeepsingh@gmail.com'


# def send_email_notification(subject, message):
#     # Create an SMTP session
#     server = smtplib.SMTP(SMTP_SERVER, SMTP_PORT)
#     server.starttls()  # Secure the connection

#     # Log in to the email account
#     server.login(SENDER_EMAIL, SENDER_PASSWORD)

#     # Create the email content
#     email_msg = MIMEMultipart()
#     email_msg['From'] = SENDER_EMAIL
#     email_msg['To'] = RECIPIENT_EMAIL
#     email_msg['Subject'] = subject
#     email_msg.attach(MIMEText(message, 'plain'))

#     # Send the email
#     server.sendmail(SENDER_EMAIL, RECIPIENT_EMAIL, email_msg.as_string())
#     print("Email alert sent successfully.")

#     # Close the server connection
#     server.quit()

# Specify the CSV file paths
csv_file = '/Users/akshdeep/Documents/Project/supermarket_sales.csv'
output_csv_file = '/Users/akshdeep/Documents/Project/cleaned_sales_inventory.csv'

# Step 1: Clean and validate data
sales_df = clean_and_validate_data(csv_file)

# Step 2: Create PostgreSQL engine
engine = create_engine('postgresql://postgres:postgresp@localhost:5432/sales_inventory_db')

# Step 3: Check for existing invoice IDs and their sale_date in the database to avoid duplicates
existing_sales = pd.read_sql_query("SELECT invoice_id, sale_date FROM sales", engine)

# Ensure both sale_date columns are in datetime format
existing_sales['sale_date'] = pd.to_datetime(existing_sales['sale_date'])
sales_df['sale_date'] = pd.to_datetime(sales_df['sale_date'])

# Filter out rows with matching invoice_id and sale_date (to avoid inserting duplicates)
sales_df = pd.merge(sales_df, existing_sales, how='left', on=['invoice_id', 'sale_date'], indicator=True)
sales_df = sales_df[sales_df['_merge'] == 'left_only']  # Only keep rows that are new or updated
sales_df.drop(columns='_merge', inplace=True)

# Step 4: Ensure that 'date' in sales_df is converted to 'sale_date' for insertion
if 'date' in sales_df.columns:
    sales_df.rename(columns={'date': 'sale_date'}, inplace=True)

# Function to insert data into the 'sales' table
def insert_sales():
    with engine.connect() as connection:
        query = text("""
        INSERT INTO sales (invoice_id, sale_date, quantity_sold, total_sale, branch, payment_method, city, customer_type)
        SELECT invoice_id, sale_date, quantity_sold, total_sale, branch, payment_method, city, customer_type
        FROM sales_df
        WHERE NOT EXISTS (SELECT 1 FROM sales s WHERE s.invoice_id = sales_df.invoice_id);
        """)
        connection.execute(query)
        print("Sales data inserted successfully.")

# Function to insert data into the 'inventory' table
def insert_inventory():
    with engine.connect() as connection:
        query = text("""
        INSERT INTO inventory (invoice_id, stock_quantity, restock_date, product_name, quantity)
        SELECT invoice_id, quantity, sale_date, product_line, quantity
        FROM sales_df
        WHERE NOT EXISTS (SELECT 1 FROM inventory i WHERE i.invoice_id = sales_df.invoice_id);
        """)
        connection.execute(query)
        print("Inventory data inserted successfully.")

# Check if sales_df is not empty before inserting into the database
if not sales_df.empty:
    try:
        # Step 5: Ingest data into 'products' table
        sales_df.to_sql('products', engine, if_exists='append', index=False)
        print("Data ingested successfully!")

        # Step 6: Insert the data into the 'sales' and 'inventory' tables
        insert_sales()
        insert_inventory()
        # # for Alerts: Send Email Notification
        # subject = "Data Ingestion Completed"
        # message = (
        #     "The data ingestion process has successfully completed. "
        #     "New sales and inventory data have been ingested into the database.\n\n"
        #     "You can check the updated data in the PostgreSQL database or on the backend server."
        # )
        # send_email_notification(subject, message)

    except Exception as e:
        print(f"An error occurred during data ingestion: {e}")
else:
    print("No new data to ingest. All records already exist in the database.")

# Fetch the latest data from the sales, products, and inventory tables
def fetch_data_from_db():
    products_df = pd.read_sql_query("SELECT * FROM products", engine)
    sales_df = pd.read_sql_query("SELECT * FROM sales", engine)
    inventory_df = pd.read_sql_query("SELECT * FROM inventory", engine)
    return products_df, sales_df, inventory_df

# Save data to CSV
def save_to_csv(products_df, sales_df, inventory_df, csv_file):
    products_df.to_csv(csv_file, mode='w', header=True, index=False)
    sales_df.to_csv(csv_file, mode='a', header=False, index=False)
    inventory_df.to_csv(csv_file, mode='a', header=False, index=False)

# Define the output path for the forecast results
forecast_csv_file = '/Users/akshdeep/Documents/Project/forecast_data.csv'

# Call the forecasting function and save the results
def forecast_and_save():
    # Fetch the latest sales_df from the database for sales forecasting
    sales_df = pd.read_sql_query("SELECT * FROM sales", engine)
    
    # Fetch the inventory data from the database for inventory forecasting
    inventory_df = pd.read_sql_query("SELECT * FROM inventory", engine)
    
    # Use the fetched sales_df and inventory_df for forecasting
    forecast_sales_df = data_forecasting.forecast_sales(sales_df)
    forecast_inventory_df = data_forecasting.forecast_inventory(inventory_df)

    # Ensure forecast columns align with database table structure
    forecast_sales_df['forecast_month'] = pd.to_datetime(forecast_sales_df['forecast_month'], format='%Y-%m').dt.strftime('%Y-%m-%d')

    # Save forecast results to CSV
    forecast_df = pd.concat([forecast_sales_df, forecast_inventory_df], axis=1)
    forecast_df.to_csv(forecast_csv_file, index=False)
    print("Forecast saved successfully.")

    # Save forecast results to their respective tables
    forecast_sales_df.to_sql('forecast_sales', engine, if_exists='replace', index=False)
    forecast_inventory_df.to_sql('forecast_inventory', engine, if_exists='replace', index=False)

# Call the forecasting functions after data ingestion
forecast_and_save()

# Fetch data and save to CSV
products_df, updated_sales_df, inventory_df = fetch_data_from_db()
save_to_csv(products_df, updated_sales_df, inventory_df, output_csv_file)
