import argparse
from datetime import datetime, timedelta
import os
import yfinance as yf
import pandas as pd

def crawl_monthly_stock(tickers=['MSFT', 'NVDA', 'GOOGL'], period="5d", save_dir="."):
    """
    Args:
        tickers (list): List of stock tickers.
        period (str): Period to download data for. Defaults to "6mo". Choice: [1d,5d,1mo,3mo,6mo,1y,2y,5y,10y,ytd,max]
        save_dir (str): Directory to save CSV files.
    Return:
        results (dict): Dictionary { "Ticker": DataFrame }
    """
    results = {}
    # data = yf.download(tickers, period=period, interval="1d")
    start_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    end_date = datetime.now().strftime("%Y-%m-%d")
    data = yf.download(tickers, start=start_date, end=end_date, interval="1d")

    if not os.path.exists(save_dir):
        os.makedirs(save_dir)

    for ticker in tickers:
        df_ticker = data.loc[:, (slice(None), ticker)]
        print(df_ticker)
        df_ticker.columns = df_ticker.columns.droplevel(1)  # Drop multi-index
        df_ticker.dropna(inplace=True)

        if df_ticker.empty:
            print(f"Không có đủ dữ liệu cho {ticker} sau khi loại bỏ NaN.")
            continue

        df_ticker['Ticker'] = ticker
        df_ticker.reset_index(inplace=True)
        cols = ['Ticker'] + [col for col in df_ticker.columns if col != 'Ticker']
        df_ticker = df_ticker[cols]
        df_ticker['Target'] = (df_ticker['Close'].shift(-1) > df_ticker['Close']).astype(int)
        df_ticker.dropna(inplace=True)

        if df_ticker.empty:
            print(f"Không có đủ dữ liệu cho {ticker} sau khi tạo target.")
            continue

        results[ticker] = df_ticker
        save_path = os.path.join(save_dir, f"{ticker}.csv")
        df_ticker.to_csv(save_path, index=False)
        print(f"Saved {ticker} data to {save_path}")

    return results

def merge_csv(csv_1, csv_2):
    df1 = pd.read_csv(csv_1)
    df2 = pd.read_csv(csv_2)
    merged_df = pd.concat([df1, df2], ignore_index=True)
    return merged_df

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Crawl monthly stock data and save CSVs.")
    parser.add_argument("--save-dir", type=str, default="output", help="Directory to save CSV files")
    parser.add_argument("--tickers", type=str, nargs="+", default=['MSFT', 'NVDA', 'GOOGL'], help="List of stock tickers")
    parser.add_argument("--period", type=str, default="6mo", help="Data period (e.g., 1d, 5d, 1mo, 6mo, 1y)")

    args = parser.parse_args()

    df = crawl_monthly_stock(tickers=args.tickers, period=args.period, save_dir=args.save_dir)
    print("Crawling complete.")
