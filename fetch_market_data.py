#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
fetch_market_data.py
--------------------
Fetch daily OHLCV data for indices/ETFs/FX using yfinance and save as tidy CSVs.
Usage:
  python fetch_market_data.py config.yaml

Dependencies:
  pip install yfinance pandas pyyaml

Notes:
- Yahoo Finance tickers map (examples):
    S&P 500 index: ^GSPC
    Nasdaq Composite: ^IXIC
    CSI 300 ETF (沪深300ETF): 510300.SS   (index 000300.SS is not reliably supported)
    Shanghai Composite: 000001.SS
    Shenzhen Component: 399001.SZ
    Hang Seng Index: ^HSI
    Hang Seng TECH Index: ^HSTECH
    Hang Seng TECH ETF (HK): 3033.HK
    Gold spot (XAUUSD): XAUUSD=X
    China Gold ETF (518880): 518880.SS
"""
import sys
import os
import time
import yaml
import pandas as pd
import yfinance as yf
from datetime import datetime

def load_config(path):
    with open(path, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    return cfg

def sanitize(name: str) -> str:
    # Safe filename
    keep = "-_.() "
    return "".join(c if c.isalnum() or c in keep else "_" for c in name)

def fetch_one(ticker: str, start: str, end: str, interval: str = "1d") -> pd.DataFrame:
    df = yf.download(ticker, start=start, end=end, interval=interval, auto_adjust=False, progress=False)
    if df.empty:
        return df
    df = df.rename(columns={
        "Open": "open",
        "High": "high",
        "Low": "low",
        "Close": "close",
        "Adj Close": "adj_close",
        "Volume": "volume"
    }).reset_index().rename(columns={"Date": "date"})
    df["ticker"] = ticker
    # Ensure date is ISO string
    df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")
    return df[["date","ticker","open","high","low","close","adj_close","volume"]]

def main():
    if len(sys.argv) < 2:
        print("Usage: python fetch_market_data.py config.yaml")
        sys.exit(1)
    cfg = load_config(sys.argv[1])
    out_dir = cfg.get("out_dir", "./market_data")
    os.makedirs(out_dir, exist_ok=True)
    start = cfg.get("start", "2015-01-01")
    end = cfg.get("end", datetime.today().strftime("%Y-%m-%d"))
    interval = cfg.get("interval", "1d")
    aliases = cfg.get("aliases", {})
    tickers = cfg.get("tickers", [])
    if not tickers:
        print("No tickers found in config.")
        sys.exit(1)

    all_frames = []
    for tk in tickers:
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Fetching: {tk} ...")
        try:
            df = fetch_one(tk, start, end, interval)
            if df.empty:
                print(f"  -> EMPTY / not available: {tk}")
                continue
            all_frames.append(df)
            # Save per-ticker CSV
            alias = aliases.get(tk, tk)
            fname = f"{sanitize(alias)}__{sanitize(tk)}.csv"
            df.to_csv(os.path.join(out_dir, fname), index=False, encoding="utf-8-sig")
            time.sleep(0.5)  # be gentle
        except Exception as e:
            print(f"  -> ERROR for {tk}: {e}")

    if all_frames:
        merged = pd.concat(all_frames, ignore_index=True)
        merged = merged.sort_values(["ticker","date"])
        merged.to_csv(os.path.join(out_dir, "ALL_TICKERS_MERGED.csv"), index=False, encoding="utf-8-sig")
        print(f"\nSaved {len(all_frames)} files + merged CSV in: {out_dir}")
    else:
        print("No data fetched. Check tickers or internet connectivity.")

if __name__ == "__main__":
    main()
