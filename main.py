# main.py
import os
from src.parsers import load_any_aoi
from src.clean_data import clean_aoi_data
from src.analysis import ensure_outputs_dir, defect_pareto_table, pcbs_scanned_over_time
from src.plots import plot_defect_histogram_only, plot_pcbs_scanned_time

INPUT_FILE = r"D:\deltron_internship\AOI_analytics\data\YOUR_FILE_HERE.csv"
OUT_DIR = r"D:\deltron_internship\AOI_analytics\outputs"

def main():
    ensure_outputs_dir(OUT_DIR)

    df_raw = load_any_aoi(INPUT_FILE)
    df = clean_aoi_data(df_raw)

    # Output 1: defect pareto csv + png (histogram only)
    pareto = defect_pareto_table(df, top_n=20)
    pareto_csv = os.path.join(OUT_DIR, "defect_pareto.csv")
    pareto_png = os.path.join(OUT_DIR, "defect_pareto.png")
    pareto.to_csv(pareto_csv, index=False)
    plot_defect_histogram_only(pareto, out_path=pareto_png)

    # Output 2: PCBs scanned per hour/day chart
    ts, grain = pcbs_scanned_over_time(df, hour_to_day_threshold_days=3)
    pcbs_png = os.path.join(OUT_DIR, "pcbs_scanned.png")
    plot_pcbs_scanned_time(ts, grain=grain, out_path=pcbs_png)

    print(f"✅ Done. Saved outputs in: {OUT_DIR}")

if __name__ == "__main__":
    main()
