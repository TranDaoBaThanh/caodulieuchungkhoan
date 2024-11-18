import requests
from bs4 import BeautifulSoup
import mysql.connector
import time
import schedule

# Kết nối với cơ sở dữ liệu MySQL
def connect_to_database():
    return mysql.connector.connect(
        host="localhost",
        user="root",
        password="",
        database="stock_data"
    )

# Hàm lấy giá chứng khoán từ Yahoo Finance, cụ thể là lấy giá cổ phiếu và % thay đổi
def fetch_stock_price(symbol: str):
    url = f"https://finance.yahoo.com/quote/{symbol}"
    response = requests.get(url)
    
    if response.status_code != 200:
        print(f"Không thể truy cập dữ liệu cho mã {symbol}")
        return None
    
    soup = BeautifulSoup(response.text, 'html.parser')
    
    # Tìm phần tử chứa giá hiện tại
    try:
        price = float(soup.find('fin-streamer', {'data-field': 'regularMarketPrice'}).text.replace(',', ''))
        change = soup.find('fin-streamer', {'data-field': 'regularMarketChangePercent'}).text
        return {
            'symbol': symbol,
            'price': price,
            'change': change
        }
    except AttributeError:
        print(f"Lỗi khi tìm dữ liệu cho mã {symbol}")
        return None

# Hàm lưu dữ liệu vào cơ sở dữ liệu MySQL
def save_to_database(data):
    connection = connect_to_database()
    cursor = connection.cursor()
    query = """
        INSERT INTO stock_prices (symbol, price, change_percent)
        VALUES (%s, %s, %s)
    """
    values = (data['symbol'], data['price'], data['change'])
    cursor.execute(query, values)
    connection.commit()
    cursor.close()
    connection.close()
    print(f"Lưu thành công dữ liệu cho mã {data['symbol']} vào MySQL.")

# Hàm tự động cào và lưu dữ liệu cho các mã chứng khoán
def scheduled_stock_scraping():
    stock_symbols = ["AAPL", "GOOGL", "MSFT", "TSLA", "AMZN","AMD","NVDA"]
    for symbol in stock_symbols:
        data = fetch_stock_price(symbol)
        if data:
            save_to_database(data)
        time.sleep(2)  # Thời gian nghỉ để tránh bị chặn IP

# Lên lịch thu thập dữ liệu mỗi 5 giây
schedule.every(5).seconds.do(scheduled_stock_scraping)

# Tiếp tục chạy lịch
while True:
    schedule.run_pending()
    time.sleep(5)