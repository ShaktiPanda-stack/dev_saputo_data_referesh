from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from typing import List, Dict
from pyspark.sql.functions import col,when, lit,percentile_approx

def recency_weights_calculations(weightdf, quarter_col='Quarter_label'):
    """
    Calculate recency weights based on quarters.
    Most recent quarter gets weight of 8, oldest quarter gets weight of 1.
    
    Parameters:
    -----------
    weightdf : DataFrame
        Input dataframe with quarter information
    quarter_col : str
        Column name containing quarter labels (e.g., "Q2'23")
    
    Returns:
    --------
    DataFrame with added 'weights' column
    """
    
    # 1️⃣ Get all unique quarters and sort them
    unique_quarters = (weightdf
                       .select(quarter_col)
                       .distinct()
                       .orderBy(quarter_col)
                       .collect())
    

    # Your custom quarter order
    quarters_ordered = ["Q2'23", "Q3'23", "Q4'23", "Q1'24", "Q2'24", "Q3'24", "Q4'24", "Q1'25"]

    # Extract quarter labels as a list
    quarter_list = [row[quarter_col] for row in unique_quarters]

    # Order quarter_list based on quarters_ordered
    quarter_list = [q for q in quarters_ordered if q in quarter_list]
    
    # 2️⃣ Calculate the number of unique quarters
    num_quarters = len(quarter_list)
    
    # 3️⃣ Create a mapping of quarters to weights
    quarter_to_weight = {}
    
    if num_quarters <= 8:
        # If 8 or fewer quarters, assign weights 1 to num_quarters
        for idx, quarter in enumerate(quarter_list):
            quarter_to_weight[quarter] = idx + 1
    else:
        # If more than 8 quarters
        for idx, quarter in enumerate(quarter_list):
            weight_value = num_quarters - idx
            # Most recent 8 quarters get weights 1-8, older ones get 1
            if weight_value > 8:
                quarter_to_weight[quarter] = 1
            else:
                quarter_to_weight[quarter] = weight_value
    
    # 4️⃣ Create a mapping expression using when/otherwise
    mapping_expr = None
    for quarter, weight in quarter_to_weight.items():
        if mapping_expr is None:
            mapping_expr = F.when(F.col(quarter_col) == quarter, weight)
        else:
            mapping_expr = mapping_expr.when(F.col(quarter_col) == quarter, weight)
    
    # Add otherwise clause for safety (shouldn't be needed if all quarters are covered)
    mapping_expr = mapping_expr.otherwise(1)
    
    # 5️⃣ Add weights column to dataframe
    weightdf = weightdf.withColumn('weights', mapping_expr)
    
    return weightdf


def weight_duplication(actualdf):
    #  Create a column with an array of size 'weights' filled with dummy values
    duplicated_df = actualdf.withColumn('dummy_array', F.expr('array_repeat(1, weights)'))

    # Explode the array to duplicate rows
    duplicated_df = duplicated_df.withColumn('dummy', F.explode('dummy_array'))

    #  Drop the helper columns
    duplicated_df = duplicated_df.drop('dummy_array', 'dummy','weights', 'Quarter_Label')
    # duplicated_df = duplicated_df.drop('dummy_array', 'dummy','weights')

    return duplicated_df
