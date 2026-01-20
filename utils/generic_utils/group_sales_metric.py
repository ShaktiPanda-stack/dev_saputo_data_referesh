import re
from pyspark.sql.functions import col, when, percentile_approx, expr, lit
from functools import reduce


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


def groupby_statistics_fiscal_year(df, group_cols, metric_cols, stats=['median'], fiscal_year_col='Fiscal Year'):
    """
    Calculate statistics with fiscal year segmentation

    Args:
        df: Input DataFrame with both transactional data and benchmark columns
        group_cols: List of columns to group by
        metric_cols: List of metric columns to analyze
        stats: List of statistics to calculate
        fiscal_year_col: Name of the fiscal year column in the source data

    Returns:
        DataFrame with statistics segmented by fiscal year and overall ("All")
    """

    # Helper function to build aggregation expressions
    def build_agg_exprs(metric_cols, stats):
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
        
        return agg_exprs

    # Get distinct fiscal years from the data
    fiscal_years = [row[0] for row in df.select(fiscal_year_col).distinct().collect()]
    fiscal_years.sort()  # Sort years for consistent ordering

    # Build aggregation expressions
    agg_exprs = build_agg_exprs(metric_cols, stats)

    # Calculate statistics for "All" (no fiscal year filtering)
    result_all = df.groupBy([col(c) for c in group_cols]).agg(*agg_exprs)
    result_all = result_all.withColumn("fiscal_year", lit("All"))

    # Calculate statistics for each individual fiscal year
    yearly_results = []
    for year in fiscal_years:
        filtered_df = df.filter(col(fiscal_year_col) == year)
        result_year = filtered_df.groupBy([col(c) for c in group_cols]).agg(*agg_exprs)
        result_year = result_year.withColumn("fiscal_year", lit(str(year)))
        yearly_results.append(result_year)

    # Combine all results: yearly results first, then "All"
    all_results = yearly_results + [result_all]
    final_result = reduce(lambda df1, df2: df1.union(df2), all_results)

    # Reorder columns to place "fiscal_year" right after group_cols
    column_order = group_cols + ["fiscal_year"] + [c for c in final_result.columns if c not in group_cols and c != "fiscal_year"]
    final_result = final_result.select(*column_order)

    return final_result