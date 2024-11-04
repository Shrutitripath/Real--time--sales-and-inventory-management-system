import pandas as pd
import numpy as np
from statsmodels.tsa.holtwinters import ExponentialSmoothing
from sqlalchemy import create_engine

# Connect to PostgreSQL
engine = create_engine('postgresql://postgres:postgresp@localhost:5432/sales_inventory_db')

# Forecast Sales for the next 12 months
def forecast_sales(sales_df):
    # Print the first few rows of the raw DataFrame to inspect 'sale_date'
    print("Raw DataFrame before parsing 'sale_date':")
    print(sales_df.head())

    # Ensure 'sale_date' is in datetime format
    sales_df['sale_date'] = pd.to_datetime(sales_df['sale_date'], errors='coerce')

    # Print parsed 'sale_date' column data for inspection
    print("Parsed 'sale_date' column data (first 5 rows):")
    print(sales_df['sale_date'].head())

    # Count the number of valid date rows
    valid_date_count = sales_df['sale_date'].notna().sum()
    print(f"Total number of rows with valid dates: {valid_date_count}")

    # If no valid dates, raise an error
    if valid_date_count == 0:
        raise ValueError("No valid dates found after cleaning. Please check the date format in the source data.")

    # Drop rows with invalid 'sale_date' or missing 'total_sale'
    sales_df.dropna(subset=['sale_date', 'total_sale'], inplace=True)

    # Group sales by date and sum the 'total_sale' for each day
    sales_grouped = sales_df.groupby('sale_date')['total_sale'].sum().reset_index()

    # Check if the grouped sales data is empty
    if sales_grouped.empty:
        raise ValueError("Sales data is empty after grouping by date. Please check your data source.")

    # Apply Exponential Smoothing with trend and seasonal components
    # Adjust seasonal_periods based on your data (e.g., 12 for monthly data over 1 year)
    try:
        model = ExponentialSmoothing(sales_grouped['total_sale'], trend='add', seasonal='add', seasonal_periods=12)
        model_fit = model.fit()
    except Exception as e:
        print(f"Error fitting model: {e}")
        return None

    # Forecast for the next 12 months
    forecast_sales = model_fit.forecast(steps=12)

    # Create a DataFrame for forecasted results
    forecast_sales_df = pd.DataFrame({
        'forecast_month': pd.date_range(start=sales_grouped['sale_date'].max(), periods=12, freq='M'),
        'forecast_sales_next_month': forecast_sales.values
    })

    return forecast_sales_df

# Forecast Inventory for each product for the next month
def forecast_inventory(inventory_df):
    # Print available columns to verify the presence of 'product_name'
    print("Available columns in inventory_df:", inventory_df.columns)

    # Ensure restock_date column is in datetime format
    inventory_df['restock_date'] = pd.to_datetime(inventory_df['restock_date'])

    # Group inventory data by product_name and restock_date using daily frequency ('D')
    product_demand = inventory_df.groupby(['product_name', 'restock_date']).agg({'stock_quantity': 'sum'})

    inventory_forecast = []

    # Loop through each product and apply forecasting
    for product in product_demand.index.get_level_values('product_name').unique():
        product_data = product_demand.loc[product]

        # Ensure that the restock_date index has a frequency set (apply asfreq only to restock_date)
        product_data = product_data.asfreq('D')

        if product_data.empty or len(product_data) < 2:  # Skip products with fewer than 2 data points
            print(f"Skipping product '{product}' due to insufficient data points.")
            continue  # Skip if no data or not enough data points

        try:
            # Apply Exponential Smoothing without seasonal component (use only trend)
            model = ExponentialSmoothing(product_data['stock_quantity'], trend='add', seasonal=None)
            fitted_model = model.fit(smoothing_level=0.8, smoothing_slope=0.2)  # Adjust parameters as needed

            # Forecast for the next month
            forecast = fitted_model.forecast(steps=1)

            # Append results to list, using .iloc[0] for safe future use
            inventory_forecast.append({
                'product_name': product,
                'forecast_month': pd.to_datetime(product_data.index[-1] + pd.DateOffset(months=1)),
                'forecast_inventory': forecast.iloc[0]  # Use iloc to access by position safely
            })
        except Exception as e:
            print(f"Error forecasting for product '{product}': {e}")
            continue  # Skip this product in case of any issues

    # Convert to DataFrame
    inventory_forecast_df = pd.DataFrame(inventory_forecast)

    return inventory_forecast_df

# Save the forecast data to CSV or PostgreSQL (for demo purpose, saving to CSV)
def save_forecasts(sales_forecast_df, inventory_forecast_df):
    sales_forecast_df.to_csv('sales_forecast.csv', index=False)
    inventory_forecast_df.to_csv('inventory_forecast.csv', index=False)
    print("Forecasts saved successfully.")

# Main function to forecast sales and inventory
def forecast_and_save():
    # Fetch sales data from PostgreSQL
    sales_df = pd.read_sql('SELECT * FROM sales', engine)

    # Print the first few rows of the raw DataFrame to ensure data is loaded correctly
    print("Raw sales DataFrame fetched from the database:")
    print(sales_df.head())

    # If sales_df is empty, raise an error
    if sales_df.empty:
        raise ValueError("Sales data is empty. Please check your data source or database connection.")
    
    # Proceed with sales forecasting
    sales_forecast_df = forecast_sales(sales_df)

    # Forecast inventory for each product
    inventory_forecast_df = forecast_inventory(sales_df)

    # Save forecasts
    save_forecasts(sales_forecast_df, inventory_forecast_df)

if __name__ == "__main__":
    forecast_and_save()
