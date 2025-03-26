import yfinance as yf
import pandas as pd

# Danh sách cổ phiếu cần lấy dữ liệu
stocks = ["NVDA", "GOOGL", "MSFT"]

# Thời gian lấy dữ liệu (1 năm gần nhất)
start_date = "2000-01-01"
end_date = "2025-03-25"

# Lấy dữ liệu cho từng cổ phiếu
for stock in stocks:
    df = yf.download(stock, start=start_date, end=end_date, interval="1mo")  # Lấy dữ liệu theo tháng
    df.to_csv(f"{stock}_1year_monthly.csv")  # Lưu vào file CSV
    print(f"Đã lưu dữ liệu {stock} vào file {stock}_1year_monthly.csv")

# Hiển thị dữ liệu của Nvidia
df_nvda = pd.read_csv("NVDA_1year_monthly.csv")
print(df_nvda.head())
