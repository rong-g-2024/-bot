import pymysql
import pandas as pd
from datetime import datetime, timedelta
import re

DB_HOST = "101.126.85.80"
DB_PORT = 3306
DB_USER = "data-analyst"
DB_PASSWORD = "MM0946^^^"
DB_NAME = "erp"

sql = """
SELECT
    p.造好物,
    COUNT(DISTINCT tg.tradeNo) AS 订单,
    SUM(tg.good_quantity) AS 商品,
    tg.currentQuantity AS 库存,
    tg.skuBarcode AS SKU,
    tg.good_name AS 名称
FROM
    (
        SELECT '今天履约！' AS 造好物
        UNION ALL SELECT '差1天逾期'
        UNION ALL SELECT '差2天逾期'
        UNION ALL SELECT '差7天逾期'
        UNION ALL SELECT '已经逾期！'
    ) p
LEFT JOIN trade_goods tg ON
    CASE
        WHEN DATEDIFF(CURDATE(), tg.lastShipTime) = 0 THEN '今天履约！'
        WHEN DATEDIFF(CURDATE(), tg.lastShipTime) = -1 THEN '差1天逾期'
        WHEN DATEDIFF(CURDATE(), tg.lastShipTime) = -2 THEN '差2天逾期'
        WHEN DATEDIFF(CURDATE(), tg.lastShipTime) BETWEEN -7 AND -3 THEN '差7天逾期'
        WHEN DATEDIFF(CURDATE(), tg.lastShipTime) > 1 THEN '已经逾期！'
    END = p.造好物
    AND tg.tradeStatusExplain = '待审核'
    AND tg.shopName = 'MakeItReal'
GROUP BY
    p.造好物, tg.good_name, tg.skuBarcode, tg.currentQuantity
ORDER BY
    FIELD(p.造好物, '今天履约！', '差1天逾期', '差2天逾期', '差7天逾期', '已经逾期！'),
    名称;
"""

def get_shipping_deadline(overdue_type):
    match = re.search(r'差(\d+)天逾期', overdue_type)
    if match:
        overdue_days = int(match.group(1))
        today = datetime.today()
        deadline = today + timedelta(days=overdue_days)
        return deadline.strftime('%Y/%m/%d')
    return datetime.today().strftime('%Y/%m/%d')

def process_overdue_days(overdue_type, df):
    overdue_data = df[df['造好物'] == overdue_type]
    deadline = get_shipping_deadline(overdue_type)

    if not overdue_data.empty:
        total_order = overdue_data['订单'].sum()
        total_skus = overdue_data['SKU'].nunique()
        total_quantity = overdue_data['商品'].sum()

        print(f"{overdue_type} 截至 {deadline} 需要发货：")

        if total_order == 0 or total_quantity == 0:
            print("无订单待履约")
            print()
            return

        print(f"订单总数：{total_order} | SKU 总数: {total_skus} | 商品总数量: {total_quantity}")
        output = [f"{row['SKU']}（{row['商品']}）" for _, row in overdue_data.iterrows()]
        print(", ".join(output))
        print()
    else:
        print(f"无“{overdue_type}”的记录")
        print()


def process_already_overdue(df):
    overdue_data = df[df['造好物'] == '已经逾期！']

    if not overdue_data.empty:
        total_order = overdue_data['订单'].sum()
        total_skus = overdue_data['SKU'].nunique()
        total_quantity = overdue_data['商品'].sum()

        print("⚠️ 已经逾期！请尽快发货！")

        if total_order == 0 or total_quantity == 0:
            print("无订单待履约")
            print()
            return

        print(f"订单总数：{total_order} | SKU 总数: {total_skus} | 商品总数量: {total_quantity}")
        output = [f"{row['SKU']}（{row['商品']}）" for _, row in overdue_data.iterrows()]
        print(", ".join(output))
        print()
    else:
        print("无“已经逾期！”的记录")
        print()


def process_today_shipment(df):
    today_data = df[df['造好物'] == '今天履约！']
    deadline = datetime.today().strftime('%Y/%m/%d')

    if not today_data.empty:
        total_order = today_data['订单'].sum()
        total_skus = today_data['SKU'].nunique()
        total_quantity = today_data['商品'].sum()

        print(f"今天履约！ 截至 {deadline} 需要发货：")

        if total_order == 0 or total_quantity == 0:
            print("无订单待履约")
            print()
            return

        print(f"订单总数：{total_order} | SKU 总数: {total_skus} | 商品总数量: {total_quantity}")
        output = [f"{row['SKU']}（{row['商品']}）" for _, row in today_data.iterrows()]
        print(", ".join(output))
        print()
    else:
        print("无“今天履约！”的记录")
        print()



def main():
    connection = None
    try:
        connection = pymysql.connect(
            host=DB_HOST,
            port=DB_PORT,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME,
            charset='utf8mb4',
            cursorclass=pymysql.cursors.DictCursor
        )

        with connection.cursor() as cursor:
            cursor.execute(sql)
            result = cursor.fetchall()

        df = pd.DataFrame(result)
        process_today_shipment(df)
        process_overdue_days('差1天逾期', df)
        process_overdue_days('差2天逾期', df)
        process_overdue_days('差7天逾期', df)
        process_already_overdue(df)

    except Exception as e:
        print(f"发生错误：{e}")

    finally:
        if connection:
            connection.close()

if __name__ == "__main__":
    main()
