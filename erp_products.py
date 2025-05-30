import pymysql
import pandas as pd
from datetime import datetime

DB_HOST = "101.126.85.80"
DB_PORT = 3306
DB_USER = "data-analyst"
DB_PASSWORD = "MM0946^^^"
DB_NAME = "erp"

conn = pymysql.connect(
    host=DB_HOST,
    port=DB_PORT,
    user=DB_USER,
    password=DB_PASSWORD,
    database=DB_NAME,
    charset="utf8mb4"
)

cursor = conn.cursor()

query = "SELECT * FROM trade_data"
cursor.execute(query)
data = cursor.fetchall()


df = pd.DataFrame(data, columns=["tradeNo", "tradeTime", "goodslist", "lastShipTime", "orderNo", "tradeStatus", 
                                 "tradeStatusExplain", "shopName", "logisticName", "shopId", "payment", "gmtCreate", "flagNames","daysSinceLastShip","hoursSinceLastShip"])

def split_and_transform(row):
    if not row['goodslist'] or pd.isna(row['goodslist']):
        return pd.DataFrame()

    goods = row['goodslist'].split(',')
    split_rows = []

    try:
        last_ship = pd.to_datetime(row['lastShipTime'], errors='coerce')  
    except Exception as e:
        last_ship = None
        print(f"Error parsing lastShipTime: {e}")

    if pd.isna(last_ship):
        return pd.DataFrame() 

    now = datetime.now()
    try:
        days_diff = (now.date() - last_ship.date()).days if last_ship else None
        hours_diff = int((now - last_ship).total_seconds() // 3600) if last_ship else None
    except:
        days_diff = None
        hours_diff = None

    for item in goods:
        try:
            good_name = item.split('(')[0].strip()
            good_quantity = int(item.split('(')[1].replace(')', '').strip())
        except IndexError:
            good_name = item.strip()
            good_quantity = 0

        split_rows.append({
            'tradeNo': row['tradeNo'],
            'orderNo': row['orderNo'],
            'shopName': row['shopName'],
            'tradeStatusExplain': row['tradeStatusExplain'],
            'good_name': good_name,
            'good_quantity': good_quantity,
            'tradeTime': row['tradeTime'],
            'lastShipTime': last_ship,
            'daysSinceLastShip': days_diff,
            'hoursSinceLastShip': hours_diff
        })

    return pd.DataFrame(split_rows)

result = pd.concat(df.apply(split_and_transform, axis=1).to_list(), ignore_index=True)

result = result.dropna(subset=['lastShipTime'])

insert_query = """
    INSERT INTO trade_goods (
        tradeNo, orderNo, shopName, tradeStatusExplain, good_name, good_quantity, 
        tradeTime, lastShipTime, daysSinceLastShip, hoursSinceLastShip
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
        tradeStatusExplain = VALUES(tradeStatusExplain),
        good_quantity = VALUES(good_quantity),
        tradeTime = VALUES(tradeTime),
        lastShipTime = VALUES(lastShipTime),
        daysSinceLastShip = VALUES(daysSinceLastShip),
        hoursSinceLastShip = VALUES(hoursSinceLastShip),
        shopName = VALUES(shopName)
"""

insert_values = []
for _, row in result.iterrows():
    insert_values.append((
        row['tradeNo'], 
        row['orderNo'], 
        row['shopName'], 
        row['tradeStatusExplain'], 
        row['good_name'], 
        row['good_quantity'], 
        row['tradeTime'], 
        row['lastShipTime'], 
        row['daysSinceLastShip'], 
        row['hoursSinceLastShip']
    ))

try:
    cursor.executemany(insert_query, insert_values)
    conn.commit()
    print("Trade_goods插入/更新成功！")
except pymysql.MySQLError as e:
    print(f"Trade_goods插入/更新数据时出错: {e}")
    conn.rollback()
finally:
    cursor.close()
    conn.close()
