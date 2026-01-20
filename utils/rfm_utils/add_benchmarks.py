from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, broadcast
from typing import List, Dict

def add_benchmark_columns(
    results_df: DataFrame,
    benchmarks_df: DataFrame,
    cost_columns: List[str] = None
) -> DataFrame:
    """
    Add benchmark columns to results dataframe based on cost column mappings.
    
    Parameters:
    -----------
    results_df : DataFrame
        Main results dataframe containing cost columns
    benchmarks_df : DataFrame
        Benchmarks dataframe with columns: cohort, cost_format, cost_metric, benchmark
    cost_columns : List[str], optional
        List of cost columns to add benchmarks for. If None, will process all matching columns.
        
    Returns:
    --------
    DataFrame
        Results dataframe with benchmark columns added next to original cost columns
    
    Example:
    --------
    cost_columns = [
        'tier_2_pct_sales', 'tier_2_per_lb', 'tier_3_pct_sales', 'tier_3_per_lb',
        'tier_2_3_pct_sales', 'tier_2_3_per_lb', 'enterprise_profit_pct_sales',
        'enterprise_profit_per_lb', 'interplant_logistics_pct_sales',
        'interplant_logistics_per_lb', 'plant_ops_pct_sales', 'plant_ops_per_lb',
        'cost_to_serve_pct_sales', 'cost_to_serve_per_lb'
    ]
    
    result = add_benchmark_columns(results_df, benchmarks_df, cost_columns)
    """
    
    # Define mapping from column suffix to cost_format
    format_mapping = {
        '_pct_sales': '% Sales',
        '_per_lb': '$/lb',
        '': 'Actual $'  # For columns without suffix (like enterprise_profit)
    }
    
    # Extract cost metric from column name
    def parse_column_name(col_name: str) -> Dict[str, str]:
        """Parse column name to extract cost_metric and cost_format"""
        for suffix, cost_format in format_mapping.items():
            if suffix and col_name.endswith(suffix):
                cost_metric = col_name.replace(suffix, '')
                return {'cost_metric': cost_metric, 'cost_format': cost_format}
        
        # If no suffix found, assume Actual $
        return {'cost_metric': col_name, 'cost_format': 'Actual $'}
    
    # If cost_columns not provided, detect them from results_df
    if cost_columns is None:
        # Get all numeric columns that match cost patterns
        all_columns = results_df.columns
        cost_columns = [
            c for c in all_columns 
            if any(c.endswith(suffix) for suffix in ['_pct_sales', '_per_lb']) 
            or c in ['cost_to_serve', 'enterprise_profit', 'tier_2', 'tier_3', 
                     'tier_2_3', 'interplant_logistics', 'plant_ops']
        ]
    
    # Identify the cohort column in results_df (distribution_ibp in the sample)
    cohort_col = 'distribution_ibp'
    
    # Start with original dataframe
    result_df = results_df
    
    # Get all columns to maintain order
    original_columns = result_df.columns
    
    # Process each cost column
    for cost_col in cost_columns:
        if cost_col not in result_df.columns:
            continue
            
        # Parse column name to get cost_metric and cost_format
        parsed = parse_column_name(cost_col)
        cost_metric = parsed['cost_metric']
        cost_format = parsed['cost_format']
        
        # Filter benchmarks for this specific cost_metric and cost_format
        filtered_benchmarks = benchmarks_df.filter(
            (col('cost_metric') == cost_metric) & 
            (col('cost_format') == cost_format)
        ).select('cohort', 'benchmark')
        
        # Rename columns to avoid conflicts
        filtered_benchmarks = filtered_benchmarks.withColumnRenamed(
            'cohort', f'{cost_col}_cohort'
        ).withColumnRenamed(
            'benchmark', f'{cost_col}_benchmark'
        )
        
        # Join with results dataframe using broadcast for efficiency
        result_df = result_df.join(
            broadcast(filtered_benchmarks),
            result_df[cohort_col] == filtered_benchmarks[f'{cost_col}_cohort'],
            'left'
        ).drop(f'{cost_col}_cohort')
    
    # Reorder columns to place benchmarks next to original columns
    reordered_columns = []
    for col_name in original_columns:
        reordered_columns.append(col_name)
        benchmark_col = f'{col_name}_benchmark'
        if benchmark_col in result_df.columns:
            reordered_columns.append(benchmark_col)
    
    # Add any remaining columns that weren't in original order
    for col_name in result_df.columns:
        if col_name not in reordered_columns:
            reordered_columns.append(col_name)
    
    result_df = result_df.select(*reordered_columns)
    
    return result_df