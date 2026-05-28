import json
import os
from pathlib import Path

import joblib
import matplotlib.gridspec as gridspec
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    abs as spark_abs,
    coalesce,
    col,
    count,
    countDistinct,
    dayofweek,
    first,
    hour,
    length,
    lit,
    log1p,
    mean,
    month,
    row_number,
    sum as spark_sum,
    to_timestamp,
    trim,
    when,
)
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import IsolationForest
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, RobustScaler


DATASET_PATH = os.getenv("DATASET_PATH", "/app/data/ecommerce.csv")
MODEL_OUTPUT_PATH = os.getenv("MODEL_OUTPUT_PATH", "/app/models/fraud_detection_pipeline.pkl")
REPORT_DIR = os.getenv("REPORT_DIR", "/app/reports")
RANDOM_SEED = int(os.getenv("RANDOM_SEED", "42"))
MAX_TRAIN_ROWS = int(os.getenv("MAX_TRAIN_ROWS", "200000"))

MODEL_PARAMS = {
    "contamination": float(os.getenv("CONTAMINATION", "0.02")),
    "n_estimators": int(os.getenv("N_ESTIMATORS", "200")),
    "max_samples": os.getenv("MAX_SAMPLES", "auto"),
    "max_features": float(os.getenv("MAX_FEATURES", "1.0")),
    "random_state": RANDOM_SEED,
    "n_jobs": -1,
}

SCALER_PARAMS = {
    "quantile_range": (10.0, 90.0),
    "with_centering": True,
    "with_scaling": True,
}

ENCODER_PARAMS = {
    "handle_unknown": "ignore",
    "max_categories": int(os.getenv("MAX_CATEGORIES", "50")),
    "sparse_output": False,
}

NUMERIC_FEATURES = [
    "quantity_log",
    "unit_price_log",
    "line_total_log",
    "price_quantity_ratio",
    "description_length",
    "invoice_hour",
    "invoice_dayofweek",
    "invoice_month",
    "is_weekend",
    "is_unusual_hour",
    "is_cancelled",
    "is_return",
    "customer_order_count",
    "customer_cancel_ratio",
    "customer_avg_price",
    "stock_frequency",
    "stock_avg_price",
    "country_frequency",
    "invoice_distinct_items",
    "invoice_total_qty",
    "invoice_total_amount",
]

CATEGORICAL_FEATURES = ["Country", "StockCode"]


def build_spark_session():
    return (
        SparkSession.builder
        .appName("fraud-model-training")
        .master("local[*]")
        .getOrCreate()
    )


def ensure_parent_dirs():
    Path(MODEL_OUTPUT_PATH).parent.mkdir(parents=True, exist_ok=True)
    Path(REPORT_DIR).mkdir(parents=True, exist_ok=True)


def dataset_overview(df):
    total_rows = df.count()
    null_counts = {}
    for column_name in df.columns:
        null_counts[column_name] = df.filter(
            col(column_name).isNull() | (trim(col(column_name).cast("string")) == "")
        ).count()

    return {
        "rows": total_rows,
        "columns": len(df.columns),
        "column_names": df.columns,
        "null_counts": null_counts,
    }


def load_raw(spark, path):
    df = spark.read.csv(path, header=True, inferSchema=False)
    return (
        df.withColumn("Quantity", col("Quantity").cast("integer"))
        .withColumn("UnitPrice", col("UnitPrice").cast("double"))
        .withColumn("CustomerID", col("CustomerID").cast("string"))
    )


def clean_basic(df):
    df = (
        df.withColumn("InvoiceNo", trim(col("InvoiceNo").cast("string")))
        .withColumn("StockCode", trim(col("StockCode").cast("string")))
        .withColumn("Description", trim(col("Description").cast("string")))
        .withColumn("Country", trim(col("Country").cast("string")))
        .withColumn("InvoiceDateParsed", to_timestamp(col("InvoiceDate"), "M/d/yyyy H:mm"))
        .withColumn("CustomerID", coalesce(col("CustomerID"), lit("GUEST")))
        .withColumn("Description", coalesce(col("Description"), lit("NO_DESC")))
        .withColumn("Country", coalesce(col("Country"), lit("UNKNOWN")))
        .withColumn("StockCode", coalesce(col("StockCode"), lit("UNKNOWN")))
    )

    return (
        df.filter(col("InvoiceDateParsed").isNotNull())
        .filter(col("Quantity").isNotNull())
        .filter(col("UnitPrice").isNotNull())
        .filter(col("UnitPrice") >= 0)
        .filter(col("Quantity") != 0)
        .filter(~((col("UnitPrice") == 0) & (col("Quantity") > 0)))
    )


def remove_duplicates(df):
    dedup_cols = ["InvoiceNo", "StockCode", "Quantity", "UnitPrice", "CustomerID"]
    window = Window.partitionBy(dedup_cols).orderBy(col("InvoiceDateParsed"))
    return (
        df.withColumn("_row_num", row_number().over(window))
        .filter(col("_row_num") == 1)
        .drop("_row_num")
    )


def cap_outliers(df):
    for column_name in ["Quantity", "UnitPrice"]:
        p01, q1, q3, p99 = df.approxQuantile(column_name, [0.01, 0.25, 0.75, 0.99], 0.01)
        iqr = q3 - q1
        lower = max(p01, q1 - 3 * iqr)
        upper = min(p99, q3 + 3 * iqr)

        df = df.withColumn(
            f"{column_name}_capped",
            when(col(column_name) < lower, lower)
            .when(col(column_name) > upper, upper)
            .otherwise(col(column_name)),
        )

    return df


def feature_engineering(df):
    df = (
        df.withColumn("invoice_hour", hour(col("InvoiceDateParsed")))
        .withColumn("invoice_dayofweek", dayofweek(col("InvoiceDateParsed")))
        .withColumn("invoice_month", month(col("InvoiceDateParsed")))
        .withColumn("is_weekend", when(dayofweek(col("InvoiceDateParsed")).isin(1, 7), 1).otherwise(0))
        .withColumn(
            "is_unusual_hour",
            when((col("invoice_hour") >= 22) | (col("invoice_hour") <= 6), 1).otherwise(0),
        )
        .withColumn("is_cancelled", when(col("InvoiceNo").startswith("C"), 1).otherwise(0))
        .withColumn("is_return", when(col("Quantity") < 0, 1).otherwise(0))
        .withColumn("quantity_abs", spark_abs(col("Quantity")))
        .withColumn("line_total", col("Quantity") * col("UnitPrice"))
        .withColumn("line_total_abs", spark_abs(col("line_total")))
        .withColumn("description_length", length(col("Description")))
        .withColumn("unit_price_log", log1p(col("UnitPrice_capped")))
        .withColumn("quantity_log", log1p(col("quantity_abs")))
        .withColumn("line_total_log", log1p(col("line_total_abs")))
        .withColumn(
            "price_quantity_ratio",
            when(col("quantity_abs") > 0, col("UnitPrice_capped") / col("quantity_abs")).otherwise(0.0),
        )
    )

    customer_stats = (
        df.groupBy("CustomerID")
        .agg(
            count("*").alias("customer_order_count"),
            spark_sum(when(col("is_cancelled") == 1, 1).otherwise(0)).alias("customer_cancel_count"),
            mean("UnitPrice_capped").alias("customer_avg_price"),
        )
        .withColumn(
            "customer_cancel_ratio",
            col("customer_cancel_count") / col("customer_order_count"),
        )
    )

    stock_stats = (
        df.groupBy("StockCode")
        .agg(
            count("*").alias("stock_frequency"),
            mean("UnitPrice_capped").alias("stock_avg_price"),
        )
    )

    country_stats = df.groupBy("Country").agg(count("*").alias("country_frequency"))

    invoice_stats = (
        df.groupBy("InvoiceNo")
        .agg(
            countDistinct("StockCode").alias("invoice_distinct_items"),
            spark_sum("quantity_abs").alias("invoice_total_qty"),
            spark_sum("line_total_abs").alias("invoice_total_amount"),
        )
    )

    enriched = (
        df.join(customer_stats, on="CustomerID", how="left")
        .join(stock_stats, on="StockCode", how="left")
        .join(country_stats, on="Country", how="left")
        .join(invoice_stats, on="InvoiceNo", how="left")
    )

    return (
        enriched.fillna(
            {
                "customer_order_count": 1,
                "customer_cancel_count": 0,
                "customer_avg_price": 1.0,
                "customer_cancel_ratio": 0.0,
                "stock_frequency": 1,
                "stock_avg_price": 1.0,
                "country_frequency": 1,
                "invoice_distinct_items": 1,
                "invoice_total_qty": 1.0,
                "invoice_total_amount": 1.0,
            }
        ),
        customer_stats,
        stock_stats,
        country_stats,
        invoice_stats,
    )


def preprocessing_summary(raw_df, clean_df):
    raw_count = raw_df.count()
    clean_count = clean_df.count()
    dropped_count = raw_count - clean_count
    return {
        "raw_rows": raw_count,
        "clean_rows": clean_count,
        "dropped_rows": dropped_count,
        "drop_ratio": round(dropped_count / raw_count, 6) if raw_count else 0.0,
        "cancelled_rows": clean_df.filter(col("is_cancelled") == 1).count(),
        "return_rows": clean_df.filter(col("is_return") == 1).count(),
        "guest_customer_rows": clean_df.filter(col("CustomerID") == "GUEST").count(),
    }


def sample_for_training(clean_df):
    clean_count = clean_df.count()
    if clean_count <= MAX_TRAIN_ROWS:
        return clean_df, clean_count, clean_count

    fraction = MAX_TRAIN_ROWS / clean_count
    sampled = clean_df.sample(withReplacement=False, fraction=fraction, seed=RANDOM_SEED)
    return sampled, clean_count, sampled.count()


def build_pipeline():
    preprocessor = ColumnTransformer(
        transformers=[
            ("num", RobustScaler(**SCALER_PARAMS), NUMERIC_FEATURES),
            ("cat", OneHotEncoder(**ENCODER_PARAMS), CATEGORICAL_FEATURES),
        ],
        remainder="drop",
        verbose_feature_names_out=False,
    )

    return Pipeline(
        steps=[
            ("preprocessor", preprocessor),
            ("model", IsolationForest(**MODEL_PARAMS)),
        ],
        verbose=False,
    )


def train(train_pdf):
    pipeline = build_pipeline()
    feature_columns = NUMERIC_FEATURES + CATEGORICAL_FEATURES
    x_train = train_pdf[feature_columns]

    pipeline.fit(x_train)

    predictions = pipeline.predict(x_train)
    scores = pipeline.score_samples(x_train)

    results_pdf = train_pdf.copy()
    results_pdf["prediction"] = predictions
    results_pdf["anomaly_score"] = scores
    results_pdf["is_anomaly"] = (predictions == -1).astype(int)
    return pipeline, results_pdf


def plot_results(results_pdf, report_dir=REPORT_DIR):
    Path(report_dir).mkdir(parents=True, exist_ok=True)

    sns.set_theme(style="whitegrid", palette="muted")
    fig = plt.figure(figsize=(18, 14))
    fig.suptitle("Fraud Detection - Isolation Forest Results", fontsize=16, fontweight="bold", y=0.98)
    grid = gridspec.GridSpec(2, 3, figure=fig, hspace=0.4, wspace=0.35)

    normal = results_pdf[results_pdf["is_anomaly"] == 0]
    anomalies = results_pdf[results_pdf["is_anomaly"] == 1]

    ax1 = fig.add_subplot(grid[0, 0])
    ax1.hist(normal["anomaly_score"], bins=80, alpha=0.6, color="#4C72B0", label="Normal")
    ax1.hist(anomalies["anomaly_score"], bins=80, alpha=0.8, color="#DD4444", label="Anomaly")
    if not anomalies.empty:
        threshold = anomalies["anomaly_score"].max()
        ax1.axvline(threshold, color="black", linestyle="--", linewidth=1.2, label=f"Threshold ~ {threshold:.3f}")
    ax1.set_title("Anomaly Score Distribution")
    ax1.set_xlabel("Score (lower = more suspicious)")
    ax1.set_ylabel("Count")
    ax1.legend(fontsize=8)

    ax2 = fig.add_subplot(grid[0, 1])
    ax2.scatter(normal["unit_price_log"], normal["quantity_log"], alpha=0.2, s=8, color="#4C72B0", label="Normal")
    ax2.scatter(anomalies["unit_price_log"], anomalies["quantity_log"], alpha=0.7, s=20, color="#DD4444", label="Anomaly", zorder=3)
    ax2.set_title("Unit Price vs Quantity (log scale)")
    ax2.set_xlabel("log(UnitPrice)")
    ax2.set_ylabel("log(Quantity)")
    ax2.legend(fontsize=8)

    ax3 = fig.add_subplot(grid[0, 2])
    hourly = results_pdf.groupby(["invoice_hour", "is_anomaly"]).size().reset_index(name="count")
    for label, color, linestyle in [(0, "#4C72B0", "-"), (1, "#DD4444", "--")]:
        subset = hourly[hourly["is_anomaly"] == label]
        ax3.plot(
            subset["invoice_hour"],
            subset["count"],
            color=color,
            linestyle=linestyle,
            label="Normal" if label == 0 else "Anomaly",
            linewidth=1.8,
        )
    ax3.set_title("Transactions Per Hour")
    ax3.set_xlabel("Hour of day")
    ax3.set_ylabel("Count")
    ax3.set_xticks(range(0, 24, 2))
    ax3.legend(fontsize=8)

    ax4 = fig.add_subplot(grid[1, 0])
    country_rates = (
        results_pdf.groupby("Country")
        .agg(total=("is_anomaly", "count"), anomalies=("is_anomaly", "sum"))
        .assign(anomaly_rate=lambda frame: frame["anomalies"] / frame["total"])
        .query("total >= 50")
        .sort_values("anomaly_rate", ascending=False)
        .head(15)
    )
    bars = ax4.barh(country_rates.index, country_rates["anomaly_rate"], color="#DD4444", alpha=0.75)
    ax4.set_title("Top 15 Countries by Anomaly Rate\n(min 50 transactions)")
    ax4.set_xlabel("Anomaly rate")
    ax4.invert_yaxis()
    for bar, value in zip(bars, country_rates["anomaly_rate"]):
        ax4.text(bar.get_width() + 0.001, bar.get_y() + bar.get_height() / 2, f"{value:.1%}", va="center", fontsize=7)

    ax5 = fig.add_subplot(grid[1, 1])
    ax5.boxplot(
        [
            normal["line_total_log"].clip(lower=0),
            anomalies["line_total_log"].clip(lower=0) if not anomalies.empty else pd.Series([0]),
        ],
        labels=["Normal", "Anomaly"],
        patch_artist=True,
        boxprops=dict(facecolor="#4C72B0", alpha=0.5),
        medianprops=dict(color="black", linewidth=2),
        flierprops=dict(marker="o", markersize=2, alpha=0.3, color="#DD4444"),
    )
    ax5.set_title("Line Total Distribution (log scale)")
    ax5.set_ylabel("log(line_total)")

    ax6 = fig.add_subplot(grid[1, 2])
    ax6.axis("off")
    total = len(results_pdf)
    n_anomaly = int(results_pdf["is_anomaly"].sum())
    n_normal = total - n_anomaly
    anomaly_rate = n_anomaly / total if total else 0.0
    avg_score_ano = anomalies["anomaly_score"].mean() if not anomalies.empty else 0.0
    avg_score_nor = normal["anomaly_score"].mean() if not normal.empty else 0.0
    summary_text = (
        f"{'-' * 32}\n"
        f"  SUMMARY\n"
        f"{'-' * 32}\n"
        f"  Total transactions    {total:>10,}\n"
        f"  Normal                {n_normal:>10,}\n"
        f"  Anomalies detected    {n_anomaly:>10,}\n"
        f"  Anomaly rate          {anomaly_rate:>10.2%}\n"
        f"{'-' * 32}\n"
        f"  Avg score (normal)    {avg_score_nor:>10.4f}\n"
        f"  Avg score (anomaly)   {avg_score_ano:>10.4f}\n"
        f"{'-' * 32}\n"
        f"  contamination         {MODEL_PARAMS['contamination']:>10.2f}\n"
        f"  n_estimators          {MODEL_PARAMS['n_estimators']:>10}\n"
        f"  max_samples           {str(MODEL_PARAMS['max_samples']):>10}\n"
        f"  max_features          {MODEL_PARAMS['max_features']:>10.1f}\n"
        f"  scaler quantiles      {str(SCALER_PARAMS['quantile_range']):>10}\n"
        f"{'-' * 32}\n"
    )
    ax6.text(
        0.05,
        0.95,
        summary_text,
        transform=ax6.transAxes,
        fontsize=9,
        verticalalignment="top",
        fontfamily="monospace",
        bbox=dict(boxstyle="round", facecolor="#f5f5f5", alpha=0.8),
    )

    output_path = Path(report_dir) / "fraud_detection_results.png"
    fig.savefig(output_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"Plot saved -> {output_path}")


def build_model_bundle(clean_df, train_pdf, pipeline, customer_stats, stock_stats, country_stats):
    customer_stats_map = {
        row["CustomerID"]: {
            "customer_order_count": int(row["customer_order_count"]),
            "customer_cancel_count": int(row["customer_cancel_count"]),
            "customer_avg_price": float(row["customer_avg_price"]),
            "customer_cancel_ratio": float(row["customer_cancel_ratio"]),
        }
        for row in customer_stats.collect()
    }
    stock_stats_map = {
        row["StockCode"]: {
            "stock_frequency": int(row["stock_frequency"]),
            "stock_avg_price": float(row["stock_avg_price"]),
        }
        for row in stock_stats.collect()
    }
    country_frequency_map = {
        row["Country"]: int(row["country_frequency"])
        for row in country_stats.collect()
    }

    numeric_defaults = {}
    for feature_name in NUMERIC_FEATURES:
        median_value = train_pdf[feature_name].median()
        numeric_defaults[feature_name] = float(median_value) if pd.notna(median_value) else 0.0

    return {
        "pipeline": pipeline,
        "numeric_features": NUMERIC_FEATURES,
        "categorical_features": CATEGORICAL_FEATURES,
        "numeric_defaults": numeric_defaults,
        "customer_stats_map": customer_stats_map,
        "stock_stats_map": stock_stats_map,
        "country_frequency_map": country_frequency_map,
        "default_customer_order_count": int(train_pdf["customer_order_count"].median()),
        "default_customer_cancel_count": int(train_pdf["customer_cancel_count"].median()),
        "default_customer_avg_price": float(train_pdf["customer_avg_price"].median()),
        "default_customer_cancel_ratio": float(train_pdf["customer_cancel_ratio"].median()),
        "default_stock_frequency": int(train_pdf["stock_frequency"].median()),
        "default_stock_avg_price": float(train_pdf["stock_avg_price"].median()),
        "default_country_frequency": int(train_pdf["country_frequency"].median()),
    }


def write_reports(initial_info, prep_info, results_pdf, sampled_rows, total_rows):
    report_path = Path(REPORT_DIR)
    report_path.joinpath("dataset_overview.json").write_text(json.dumps(initial_info, indent=2), encoding="utf-8")
    report_path.joinpath("preprocessing_summary.json").write_text(json.dumps(prep_info, indent=2), encoding="utf-8")
    results_pdf.to_csv(report_path / "training_results.csv", index=False)
    results_pdf.sort_values("anomaly_score").head(100).to_csv(report_path / "top_suspicious_transactions.csv", index=False)
    results_pdf.head(500).to_csv(report_path / "preprocessed_preview.csv", index=False)

    anomaly_count = int(results_pdf["is_anomaly"].sum())
    summary = {
        "sampled_rows_used_for_training": int(sampled_rows),
        "available_rows_after_preprocessing": int(total_rows),
        "anomaly_count": anomaly_count,
        "anomaly_ratio": round(anomaly_count / len(results_pdf), 6) if len(results_pdf) else 0.0,
        "normal_count": int((results_pdf["is_anomaly"] == 0).sum()),
        "lowest_anomaly_scores": results_pdf["anomaly_score"].nsmallest(10).round(6).tolist(),
        "highest_anomaly_scores": results_pdf["anomaly_score"].nlargest(10).round(6).tolist(),
        "model_params": MODEL_PARAMS,
        "scaler_params": SCALER_PARAMS,
        "encoder_params": ENCODER_PARAMS,
    }
    report_path.joinpath("model_summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")


def print_console_summary(initial_info, prep_info, results_pdf, sampled_rows, total_rows):
    print("\n=== DATASET OVERVIEW ===")
    print(json.dumps(initial_info, indent=2))

    print("\n=== PREPROCESSING SUMMARY ===")
    print(json.dumps(prep_info, indent=2))

    print("\n=== TRAINING SUMMARY ===")
    print(f"Rows available after preprocessing : {total_rows}")
    print(f"Rows used for training            : {sampled_rows}")
    print(f"Anomalies detected                : {int(results_pdf['is_anomaly'].sum())}")
    print(f"Anomaly ratio                     : {results_pdf['is_anomaly'].mean():.4f}")

    print("\n=== TOP 10 SUSPICIOUS TRANSACTIONS ===")
    suspicious_columns = [
        "InvoiceNo",
        "StockCode",
        "Country",
        "Quantity",
        "UnitPrice",
        "line_total",
        "invoice_total_amount",
        "customer_cancel_ratio",
        "anomaly_score",
    ]
    print(results_pdf.sort_values("anomaly_score")[suspicious_columns].head(10).to_string(index=False))


def main():
    ensure_parent_dirs()
    spark = build_spark_session()

    raw_df = load_raw(spark, DATASET_PATH)
    initial_info = dataset_overview(raw_df)

    clean_df = clean_basic(raw_df)
    clean_df = remove_duplicates(clean_df)
    clean_df = cap_outliers(clean_df)
    clean_df, customer_stats, stock_stats, country_stats, _ = feature_engineering(clean_df)
    prep_info = preprocessing_summary(raw_df, clean_df)

    sampled_df, total_rows, sampled_rows = sample_for_training(clean_df)
    train_pdf = sampled_df.toPandas()

    pipeline, results_pdf = train(train_pdf)
    plot_results(results_pdf, REPORT_DIR)

    model_bundle = build_model_bundle(clean_df, train_pdf, pipeline, customer_stats, stock_stats, country_stats)
    joblib.dump(model_bundle, MODEL_OUTPUT_PATH)

    write_reports(initial_info, prep_info, results_pdf, sampled_rows, total_rows)
    print_console_summary(initial_info, prep_info, results_pdf, sampled_rows, total_rows)

    print(f"\nModel saved to: {MODEL_OUTPUT_PATH}")
    print(f"Reports saved to: {REPORT_DIR}")
    spark.stop()


if __name__ == "__main__":
    main()
