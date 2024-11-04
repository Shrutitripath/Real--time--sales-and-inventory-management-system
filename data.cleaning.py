import pandas as pd

def clean_and_validate_data(csv_file):
    # Load the CSV file
    sales_df = pd.read_csv(csv_file)
    
    # Step 1: Initial check for empty DataFrame
    if sales_df.empty:
        raise ValueError("The sales CSV file is empty. Please check the file.")
    
    # Step 2: Remove duplicate 'Invoice ID' entries
    sales_df.drop_duplicates(subset=['Invoice ID'], inplace=True)
    
    # Step 3: Rename the CSV columns to match the PostgreSQL table columns
    sales_df.rename(columns={
        'Invoice ID': 'invoice_id',
        'Branch': 'branch',
        'City': 'city',
        'Customer type': 'customer_type',
        'Gender': 'gender',
        'Product line': 'product_line',
        'Unit price': 'unit_price',
        'Quantity': 'quantity',
        'Tax 5%': 'tax',
        'Total': 'total',
        'Date': 'date',
        'Time': 'time',
        'Payment': 'payment',
        'cogs': 'cogs',
        'gross margin percentage': 'gross_margin_percentage',
        'gross income': 'gross_income',
        'Rating': 'rating'
    }, inplace=True)

    # Step 4: Parse 'date' column with the correct format
    if 'date' not in sales_df.columns:
        raise ValueError("The 'date' column is missing from the sales data.")
    
    # Parse the 'date' column with the format in the original file: "month/day/year"
    sales_df['date'] = pd.to_datetime(sales_df['date'], format='%m/%d/%Y', errors='coerce')
    
    # Log invalid date rows for inspection
    invalid_dates = sales_df[sales_df['date'].isna()]
    if not invalid_dates.empty:
        print(f"Found {len(invalid_dates)} rows with invalid 'date' values:")
        print(invalid_dates[['invoice_id', 'date']])
    
    # Drop rows where 'date' is NaT (invalid dates)
    sales_df.dropna(subset=['date'], inplace=True)
    
    # Step 5: Check for missing or null values in 'Invoice ID'
    # Replace the inplace fillna to avoid the FutureWarning
    sales_df['invoice_id'] = sales_df['invoice_id'].fillna('Unknown')
    
    # Step 6: Remove negative or unrealistic values for 'unit_price' and 'quantity'
    sales_df = sales_df[(sales_df['unit_price'] >= 0) & (sales_df['quantity'] >= 0)]
    
    # Step 7: Rename the 'date' column to 'sale_date' for consistency
    sales_df.rename(columns={'date': 'sale_date'}, inplace=True)
    
    print(sales_df[['invoice_id', 'sale_date']].head())  # Check parsed sale_date column

    # Final check for empty DataFrame
    if sales_df.empty:
        raise ValueError("Cleaned sales data is empty after validation. Please check your CSV or cleaning function.")
    
    # Return cleaned sales data
    return sales_df+
  
