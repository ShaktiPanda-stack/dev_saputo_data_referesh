"""
Master pipeline runner for all standalone scripts.
"""

# Importing each file's main()
from t_benchmarks_refined import main as main_file1
from t_dimensions_refined import main as main_file2
from t_recency_frequency_refined import main as main_file3
from t_sales_metric_refined import main as main_file4
from t_rfm_outliers_bad.py import main as main_file5
from t_rfm_outliers_good.py import main as main_file6


def master():
    """Run all scripts in sequence."""

    print("\n=============== FULL PIPELINE STARTED ===============")

    # -------- Run File 1 --------
    print("\n>>> Running File 1 ...")
    main_file1()
    print("✔ Completed File 1")

    # -------- Run File 2 --------
    print("\n>>> Running File 2 ...")
    main_file2()
    print("✔ Completed File 2")

    # -------- Run File 3 --------
    print("\n>>> Running File 3 ...")
    main_file3()
    print("✔ Completed File 3")

    # -------- Run File 4 --------
    print("\n>>> Running File 4 ...")
    main_file4()
    print("✔ Completed File 4")

    # -------- Run File 5 --------
    print("\n>>> Running File 5 ...")
    main_file5()
    print("✔ Completed File 5")

        # -------- Run File 6 --------
    print("\n>>> Running File 6 ...")
    main_file6()
    print("✔ Completed File 6")


    print("\n=============== FULL PIPELINE COMPLETED ===============\n")


if __name__ == "__main__":
    master()
