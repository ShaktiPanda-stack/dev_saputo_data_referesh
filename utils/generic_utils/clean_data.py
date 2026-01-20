from pyspark.sql.functions import col
from pyspark.sql import functions as F
 
 
def remove_specific_combinations(df):
    """
    Remove records where specific Order Type and IBP Group 2 combinations are satisfied
    """
    condition = (
        ((F.col("Order Type - Dairy Cheese") == "Dairy Foods") & (F.col("IBP Group 2 Text") == "Production/WIP")) |
        ((F.col("Order Type - Dairy Cheese") == "Cheese") & (F.col("IBP Group 2 Text") == "Milk Protein Concentrate(MPC)")) |
        ((F.col("Order Type - Dairy Cheese") == "Cheese") & (F.col("IBP Group 2 Text") == "NULL")) |
        ((F.col("Order Type - Dairy Cheese") == "Cheese") & (F.col("IBP Group 2 Text") == "Sour Cream"))
    )
   
    return df.filter(~condition)