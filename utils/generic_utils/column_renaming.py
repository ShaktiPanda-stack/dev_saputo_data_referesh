import re
from pyspark.sql.functions import col

# Function to clean and standardize column names
def clean_column_name(col_name):
    """
    Cleans and standardizes the given column name by applying a series of transformations.
    
    Args:
        col_name (str): The column name to be cleaned.
    
    Returns:
        str: The cleaned and standardized column name.
    """
    # Special case for 'GrossProift' to correct the spelling
    if col_name == 'GrossProift':
        col_name = 'GrossProfit'
    
    # Replace 'Rank' with 'Rank_'
    col_name = col_name.replace('Rank', 'Rank_')

    # Replace 'FTEs' with 'Ftes' to make it consistent
    col_name = col_name.replace('FTEs', 'Ftes')

    # If the column name contains '?', remove it and add "Is" at the beginning with the first letter capitalized
    if '?' in col_name:
        col_name = col_name.replace('?', '')  # Remove the '?'
        col_name = f'Is{col_name[0].upper()}{col_name[1:]}'  # Add "Is" and capitalize first letter

    # Replace multiple spaces with a single space
    col_name = ' '.join(col_name.split())  

    # Replace '&' with 'and' to make it more readable
    col_name = col_name.replace('&', 'and')

    # Remove hyphens from the column name
    col_name = col_name.replace('-', '')  

    # Replace '+' with 'plus' for better clarity
    col_name = col_name.replace('+', 'plus')  

    # Replace '/' with 'Per' to indicate a ratio
    col_name = col_name.replace('/', 'Per')  

    # Replace '%' with 'Pct' to standardize the percentage symbol
    col_name = col_name.replace('%', 'Pct')  

    # Use regular expressions to separate camel case into readable words
    col_name = re.sub(r'(?<=[a-z0-9])([A-Z])|(?<=[A-Z])([A-Z][a-z])', r' \1\2', col_name)

    # Replace spaces with underscores
    col_name = col_name.replace(' ', '_')  

    # Remove any extra underscores that may have been introduced
    col_name = col_name.replace('__', '_')  

    # Convert all characters to lowercase to maintain uniformity
    col_name = col_name.lower()  

    # Replace 'is_is_' with 'is_' (a common issue after transformations)
    col_name = col_name.replace('is_is_', 'is_')

    # Replace '1st' with 'first' to avoid confusion with numbers
    col_name = col_name.replace('1st', 'first')

    return col_name


# Apply the clean_column_name function to rename all columns in a PySpark DataFrame
def clean_column_names(df):
    """
    Cleans and standardizes all column names in the given DataFrame.
    
    Args:
        df (DataFrame): The PySpark DataFrame whose columns need to be cleaned.
    
    Returns:
        DataFrame: The DataFrame with cleaned and standardized column names.
    """
    # Apply the cleaning function to each column name
    new_columns = [clean_column_name(col_name) for col_name in df.columns]
    
    # Rename the columns in the DataFrame with the cleaned names
    return df.toDF(*new_columns)
