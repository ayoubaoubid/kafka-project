import os
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd


REPORT_DIR = Path(os.getenv("REPORT_DIR", "/app/reports"))
RESULTS_PATH = REPORT_DIR / "training_results.csv"
OUTPUT_DIR = REPORT_DIR / "figures"


def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    df = pd.read_csv(RESULTS_PATH)
    anomaly_df = df[df["is_anomaly"] == 1]
    normal_df = df[df["is_anomaly"] == 0]

    plt.figure(figsize=(10, 6))
    plt.hist(normal_df["anomaly_score"], bins=50, alpha=0.7, label="Normal")
    plt.hist(anomaly_df["anomaly_score"], bins=50, alpha=0.7, label="Anomaly")
    plt.title("Distribution of Isolation Forest Anomaly Scores")
    plt.xlabel("Anomaly score")
    plt.ylabel("Count")
    plt.legend()
    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / "anomaly_score_distribution.png", dpi=150)
    plt.close()

    plt.figure(figsize=(10, 6))
    plt.scatter(
        normal_df["quantity_abs"],
        normal_df["line_total_abs"],
        s=8,
        alpha=0.25,
        label="Normal",
    )
    plt.scatter(
        anomaly_df["quantity_abs"],
        anomaly_df["line_total_abs"],
        s=18,
        alpha=0.7,
        label="Anomaly",
    )
    plt.title("Absolute Quantity vs Absolute Line Total")
    plt.xlabel("quantity_abs")
    plt.ylabel("line_total_abs")
    plt.legend()
    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / "quantity_vs_line_total.png", dpi=150)
    plt.close()

    top_anomalies = (
        df.sort_values("anomaly_score")
        .loc[:, ["InvoiceNo", "StockCode", "Country", "Quantity", "UnitPrice", "line_total", "anomaly_score"]]
        .head(15)
    )

    fig, ax = plt.subplots(figsize=(14, 5))
    ax.axis("off")
    table = ax.table(
        cellText=top_anomalies.values,
        colLabels=top_anomalies.columns,
        loc="center",
    )
    table.auto_set_font_size(False)
    table.set_fontsize(8)
    table.scale(1, 1.4)
    plt.title("Top 15 Most Suspicious Transactions", pad=16)
    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / "top_suspicious_transactions.png", dpi=150)
    plt.close()

    print(f"Figures saved to: {OUTPUT_DIR}")


if __name__ == "__main__":
    main()
