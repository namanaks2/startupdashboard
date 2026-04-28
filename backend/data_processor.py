"""
Data processing engine.
Accepts a pandas DataFrame and returns KPIs, chart series, and forecasts.
"""
from __future__ import annotations

import io
import warnings
from datetime import datetime
from typing import Any

import numpy as np
import pandas as pd

warnings.filterwarnings("ignore")


# ── CSV Parsing ───────────────────────────────────────────────────────────────

def parse_csv(contents: bytes) -> pd.DataFrame:
    """Parse CSV bytes into a DataFrame, normalising column names."""
    df = pd.read_csv(io.BytesIO(contents))
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
    return df


def df_to_records(df: pd.DataFrame) -> list[dict]:
    return df.where(pd.notna(df), None).to_dict(orient="records")


# ── Column detection helpers ──────────────────────────────────────────────────

_DATE_HINTS = ["date", "day", "month", "week", "period", "time", "year"]
_REVENUE_HINTS = ["revenue", "sales", "income", "earnings", "gmv", "arr", "mrr"]
_USER_HINTS = ["users", "customers", "signups", "registrations", "accounts", "dau", "mau"]
_CONVERSION_HINTS = ["conversion", "conv_rate", "ctr", "rate"]
_RETENTION_HINTS = ["retention", "churn", "retained"]


def _find_col(df: pd.DataFrame, hints: list[str]) -> str | None:
    for hint in hints:
        for col in df.columns:
            if hint in col:
                return col
    return None


def _ensure_date_col(df: pd.DataFrame, date_col: str) -> pd.DataFrame:
    df = df.copy()
    df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
    df = df.dropna(subset=[date_col]).sort_values(date_col)
    return df


# ── KPI computation ───────────────────────────────────────────────────────────

def compute_kpis(df: pd.DataFrame) -> dict[str, Any]:
    kpis: dict[str, Any] = {}

    rev_col = _find_col(df, _REVENUE_HINTS)
    if rev_col:
        series = pd.to_numeric(df[rev_col], errors="coerce").dropna()
        kpis["total_revenue"] = round(float(series.sum()), 2)
        kpis["avg_revenue"] = round(float(series.mean()), 2)
        kpis["revenue_growth"] = _growth(series)

    user_col = _find_col(df, _USER_HINTS)
    if user_col:
        series = pd.to_numeric(df[user_col], errors="coerce").dropna()
        kpis["total_users"] = int(series.sum())
        kpis["user_growth"] = _growth(series)

    conv_col = _find_col(df, _CONVERSION_HINTS)
    if conv_col:
        series = pd.to_numeric(df[conv_col], errors="coerce").dropna()
        kpis["avg_conversion_rate"] = round(float(series.mean()), 2)

    ret_col = _find_col(df, _RETENTION_HINTS)
    if ret_col:
        series = pd.to_numeric(df[ret_col], errors="coerce").dropna()
        kpis["avg_retention_rate"] = round(float(series.mean()), 2)

    # Fallback numeric summary
    numeric_cols = df.select_dtypes(include="number").columns.tolist()
    kpis["numeric_summary"] = {
        col: {
            "sum": round(float(df[col].sum()), 2),
            "mean": round(float(df[col].mean()), 2),
            "min": round(float(df[col].min()), 2),
            "max": round(float(df[col].max()), 2),
        }
        for col in numeric_cols[:10]  # cap at 10
    }

    return kpis


def _growth(series: pd.Series) -> float:
    """Percentage change from first half to second half."""
    if len(series) < 2:
        return 0.0
    mid = len(series) // 2
    first = series.iloc[:mid].mean()
    second = series.iloc[mid:].mean()
    if first == 0:
        return 0.0
    return round(float((second - first) / first * 100), 2)


# ── Chart data generation ─────────────────────────────────────────────────────

def generate_chart_data(df: pd.DataFrame) -> dict[str, Any]:
    charts: dict[str, Any] = {}
    date_col = _find_col(df, _DATE_HINTS)
    numeric_cols = df.select_dtypes(include="number").columns.tolist()[:6]

    if date_col and numeric_cols:
        tdf = _ensure_date_col(df, date_col)

        # Line / Area chart — trend over time
        line_data = []
        for _, row in tdf.iterrows():
            point = {"date": str(row[date_col])[:10]}
            for col in numeric_cols:
                val = row.get(col)
                point[col] = round(float(val), 2) if pd.notna(val) else 0
            line_data.append(point)
        charts["line"] = line_data
        charts["area"] = line_data  # same data, different visual

        # Bar chart — monthly aggregation
        tdf["_month"] = tdf[date_col].dt.to_period("M").astype(str)
        bar_data = (
            tdf.groupby("_month")[numeric_cols]
            .sum()
            .reset_index()
            .rename(columns={"_month": "month"})
        )
        charts["bar"] = bar_data.to_dict(orient="records")

    # Pie chart — distribution of numeric columns (totals)
    if numeric_cols:
        totals = {col: round(float(df[col].sum()), 2) for col in numeric_cols}
        charts["pie"] = [{"name": k, "value": v} for k, v in totals.items() if v > 0]

    return charts


# ── Automated insights ────────────────────────────────────────────────────────

def generate_insights(df: pd.DataFrame, kpis: dict) -> list[str]:
    insights = []

    rev_growth = kpis.get("revenue_growth")
    if rev_growth is not None:
        direction = "grew" if rev_growth >= 0 else "declined"
        emoji = "🚀" if rev_growth >= 10 else "📈" if rev_growth > 0 else "📉"
        insights.append(
            f"{emoji} Revenue {direction} by {abs(rev_growth):.1f}% in the second half of the period, indicating strong market traction."
        )

    user_growth = kpis.get("user_growth")
    if user_growth is not None:
        direction = "expanded" if user_growth >= 0 else "contracted"
        insights.append(
            f"👥 The active user base {direction} by {abs(user_growth):.1f}%, reflecting effective acquisition strategies."
        )

    # Anomaly / Variance Detection
    numeric_cols = df.select_dtypes(include="number").columns.tolist()
    for col in numeric_cols:
        series = pd.to_numeric(df[col], errors="coerce").dropna()
        if len(series) > 3:
            mean = series.mean()
            std = series.std()
            max_val = series.max()
            if std > 0 and (max_val - mean) / std > 2:
                month = ""
                date_col = _find_col(df, _DATE_HINTS)
                if date_col:
                    try:
                        max_idx = series.idxmax()
                        month = f" in {str(df.loc[max_idx, date_col])[:7]}"
                    except:
                        pass
                # Format to a nice readable number
                val_fmt = f"{max_val:,.2f}" if max_val < 10000 else f"{max_val:,.0f}"
                insights.append(
                    f"⚠️ Detected a significant spike in '{col}'{month}, reaching {val_fmt} (more than 2 standard deviations above average)."
                )

    # Correlation Analysis
    if len(numeric_cols) >= 2 and len(df) > 3:
        corr = df[numeric_cols].corr()
        max_corr = 0
        pair = None
        for i in range(len(numeric_cols)):
            for j in range(i+1, len(numeric_cols)):
                c = corr.iloc[i, j]
                if pd.notna(c) and c > max_corr and c < 0.999: # avoid self-correlation
                    max_corr = c
                    pair = (numeric_cols[i], numeric_cols[j])
        if pair and max_corr > 0.7:
            insights.append(
                f"🔗 High positive correlation ({max_corr:.2f}) observed between '{pair[0]}' and '{pair[1]}', suggesting they drive each other."
            )

    conv = kpis.get("avg_conversion_rate")
    if conv is not None:
        if conv >= 4:
            insights.append(f"🔥 Exceptional average conversion rate of {conv:.2f}%, well above industry benchmarks.")
        elif conv < 2:
            insights.append(f"⚠️ Average conversion rate is {conv:.2f}%. Consider optimizing the onboarding funnel.")
        else:
            insights.append(f"✅ Steady conversion rate maintained at {conv:.2f}%.")

    ret = kpis.get("avg_retention_rate")
    if ret is not None:
        if ret >= 90:
            insights.append(f"💎 Outstanding user retention at {ret:.2f}%, indicating very strong product-market fit.")
        elif ret < 60:
            insights.append(f"🚨 Retention rate is low ({ret:.2f}%). Focus on user re-engagement campaigns.")

    # General numeric insights fallback
    summary = kpis.get("numeric_summary", {})
    if not rev_growth and not user_growth and summary:
        for col, stats in list(summary.items())[:2]:
            insights.append(
                f"📊 '{col}' ranges from {stats['min']} to {stats['max']} "
                f"(avg {stats['mean']}, total {stats['sum']})."
            )

    if not insights:
        insights.append("ℹ️ Upload a CSV with date, revenue, users, or conversion columns for richer insights.")

    return insights



# ── Forecasting ───────────────────────────────────────────────────────────────

def generate_forecast(df: pd.DataFrame, periods: int = 6) -> dict[str, Any]:
    """
    Simple Holt-Winters / linear extrapolation forecast.
    Returns forecast series for each key numeric column.
    """
    date_col = _find_col(df, _DATE_HINTS)
    numeric_cols = df.select_dtypes(include="number").columns.tolist()[:4]

    if not date_col or not numeric_cols:
        return {"error": "Need a date column and at least one numeric column for forecasting."}

    tdf = _ensure_date_col(df, date_col)
    forecasts: dict[str, list] = {}

    # Determine date frequency
    if len(tdf) >= 2:
        delta = (tdf[date_col].iloc[-1] - tdf[date_col].iloc[-2])
        freq_days = int(delta.days) or 30
    else:
        freq_days = 30

    last_date = tdf[date_col].iloc[-1]
    future_dates = [
        str((last_date + pd.Timedelta(days=freq_days * (i + 1))).date())
        for i in range(periods)
    ]

    for col in numeric_cols:
        series = pd.to_numeric(tdf[col], errors="coerce").fillna(0).values

        # Try Holt-Winters exponential smoothing
        try:
            from statsmodels.tsa.holtwinters import ExponentialSmoothing

            if len(series) >= 4:
                model = ExponentialSmoothing(series, trend="add").fit(optimized=True)
                forecast_vals = model.forecast(periods)
            else:
                raise ValueError("too short")
        except Exception:
            # Fallback: linear extrapolation
            x = np.arange(len(series))
            coeffs = np.polyfit(x, series, 1)
            forecast_vals = [
                max(0, coeffs[0] * (len(series) + i) + coeffs[1])
                for i in range(periods)
            ]

        forecasts[col] = [
            {"date": future_dates[i], "forecast": round(float(forecast_vals[i]), 2)}
            for i in range(periods)
        ]

    return {"periods": periods, "forecasts": forecasts, "future_dates": future_dates}
