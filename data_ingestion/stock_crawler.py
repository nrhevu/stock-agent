import yfinance as yf
import pandas as pd

def crawl_monthly_stock(tickers = ['MSFT', 'NVDA', 'GOOGL'], period="6mo"):
    """_summary_

    Args:
        tickers (list, optional): _description_. Defaults to ['MSFT', 'NVDA', 'GOOGL'].
        period (str, optional): _description_. Defaults to "6mo". Choice: [1d,5d,1mo,3mo,6mo,1y,2y,5y,10y,ytd,max]
    Return:
        results: (object): {
            "Ticker": ticker_df
        }
    """
    results = {}
    data = yf.download(tickers, period=period, interval="1d")

    # -- 2 & 3. Tiền xử lý dữ liệu --
    results = {}
    df_all = pd.DataFrame()
    for ticker in tickers:

        df_ticker = data.loc[:, (slice(None), ticker)]
        df_ticker.columns = df_ticker.columns.droplevel(1) # Bỏ multi-index cột
        df_ticker.dropna(inplace=True)

        if df_ticker.empty:
            print(f"Không có đủ dữ liệu cho {ticker} sau khi loại bỏ NaN.")
            continue

        # 3a. Tạo biến mục tiêu (Target)
        # Dự đoán giá đóng cửa tháng sau sẽ tăng (1) hay giảm/bằng (0) so với tháng này
        df_ticker['Target'] = (df_ticker['Close'].shift(-1) > df_ticker['Close']).astype(int)
        df_ticker.dropna(inplace=True)

        if df_ticker.empty:
            print(f"Không có đủ dữ liệu cho {ticker} sau khi tạo target.")
            continue
        results[ticker] = df_ticker
    return results
def merge_csv(csv_1, csv_2):
    df1 = pd.read_csv(csv_1)
    df2 = pd.read_csv(csv_2)

    # Gộp theo chiều dọc (nối dòng)
    merged_df = pd.concat([df1, df2], ignore_index=True)
    return merged_df
if __name__ == "__main__":
    df = crawl_monthly_stock()
    print(df)