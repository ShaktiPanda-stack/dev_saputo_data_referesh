import re
from pyspark.sql.functions import col,when, percentile_approx,expr


def groupby_statistics(df, group_cols, metric_cols, stats=['median']):
    """
    Calculate statistics 
    
    Args:
        df: Input DataFrame with both transactional data and benchmark columns
        group_cols: List of columns to group by
        metric_cols: List of metric columns to analyze
        stats: List of statistics to calculate
    
    Returns:
        DataFrame with statistics and benchmark metrics
    """
    
    # Build aggregation expressions
    agg_exprs = []
    
    # Standard statistics for ALL metric columns
    for col_name in metric_cols:
        for stat in stats:
            if stat == 'median':
                agg_exprs.append(percentile_approx(col(col_name), 0.5).alias(f"{col_name}_median"))
            elif stat == 'mean':
                agg_exprs.append(expr(f"avg(`{col_name}`)").alias(f"{col_name}_mean"))
            elif stat == 'max':
                agg_exprs.append(expr(f"max(`{col_name}`)").alias(f"{col_name}_max"))
            elif stat == 'min':
                agg_exprs.append(expr(f"min(`{col_name}`)").alias(f"{col_name}_min"))
            elif stat == 'count':
                agg_exprs.append(expr(f"count(`{col_name}`)").alias(f"{col_name}_count"))
    
    # Filter metric columns to exclude those ending with '_pct_sales' or '_per_lb'
    filtered_metric_cols = [
        col_name for col_name in metric_cols 
        if not (col_name.endswith('_pct_sales') or col_name.endswith('_per_lb'))
    ]
    
    # Add sum only for filtered columns
    for col_name in filtered_metric_cols:
        agg_exprs.append(expr(f"sum(`{col_name}`)").alias(f"{col_name}_sum"))
    
    # Group and aggregate
    result_df = df.groupBy([col(c) for c in group_cols]).agg(*agg_exprs)
    
    return result_df