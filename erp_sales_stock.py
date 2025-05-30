import hashlib
import json
import urllib.parse
import urllib.request
from datetime import datetime, timedelta
import os
import pandas as pd
import time
import pymysql

# 常量定义
GATEWAY = "https://open.jackyun.com/open/openapi/do"
DEFAULT_DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"
STR_UFT8 = "UTF-8"
BIZCONTENT = "bizcontent"
APP_KEY = "11439615"
SECRET_KEY = "b4746f944ae64428a40e711a56ccc45e"

# SQL DB
DB_HOST = "101.126.85.80"
DB_PORT = 3306
DB_USER = "data-analyst"
DB_PASSWORD = "MM0946^^^"
DB_NAME = "erp"

def md5_encrypt(text, encoding=STR_UFT8):
    md5 = hashlib.md5()
    md5.update(text.encode(encoding))
    return md5.hexdigest()

def post_data(url, post_data, encoding=STR_UFT8, timeout=300000):
    headers = {
        "accept": "*/*",
        "connection": "Keep-Alive",
        "user-agent": "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1;SV1)",
        "Content-Type": "application/x-www-form-urlencoded",
        "Charset": encoding
    }
    data = post_data.encode(encoding)
    req = urllib.request.Request(url, data=data, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as response:
            result = response.read().decode(encoding)
        return result
    except Exception as e:
        print(f"请求出错: {e}")
        return None

def post(method, version, biz_data):
    sorted_map = {
        "method": method,
        "appkey": APP_KEY,
        "version": version,
        "contenttype": "json",
        "timestamp": datetime.now().strftime(DEFAULT_DATETIME_FORMAT),
        BIZCONTENT: biz_data
    }

    sign_data = SECRET_KEY
    for key, value in sorted(sorted_map.items()):
        sign_data += key + value
    sign_data += SECRET_KEY

    sorted_map["sign"] = md5_encrypt(sign_data.lower())

    post_data_str = ""
    for key, value in sorted_map.items():
        if key == BIZCONTENT:
            value = urllib.parse.quote(value, encoding=STR_UFT8)
        if post_data_str:
            post_data_str += "&"
        post_data_str += f"{key}={value}"
    return post_data(GATEWAY, post_data_str)

def save_to_db_trade(data):
    """将交易数据保存到MySQL数据库的trade_data表"""
    try:
        connection = pymysql.connect(
            host=DB_HOST,
            port=DB_PORT,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME
        )
        cursor = connection.cursor()
        insert_query = """
        INSERT INTO trade_data (
            tradeNo, tradeTime, goodslist, tradeStatus, tradeStatusExplain, shopName, logisticName, shopId, payment, gmtCreate, flagNames, lastShipTime, orderNo 
        ) 
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)  
        ON DUPLICATE KEY UPDATE
            tradeStatus = VALUES(tradeStatus),
            tradeTime = VALUES(tradeTime),
            goodslist = VALUES(goodslist),
            tradeStatusExplain = VALUES(tradeStatusExplain),
            shopName = VALUES(shopName),
            logisticName = VALUES(logisticName),
            shopId = VALUES(shopId),
            payment = VALUES(payment),
            gmtCreate = VALUES(gmtCreate),
            flagNames = VALUES(flagNames),
            lastShipTime = VALUES(lastShipTime),
            orderNo = VALUES(orderNo)
        """
        for row in data:
            trade_no = row.get("tradeNo", None) if row.get("tradeNo", "") != "" else None
            tradeTime = row.get("tradeTime", None) if row.get("tradeTime", "") != "" else None
            goodslist = row.get("goodslist", None) if row.get("goodslist", "") != "" else None
            flag_names = row.get("flagNames", None) if row.get("flagNames", "") != "" else None
            trade_status = row.get("tradeStatus", None) if row.get("tradeStatus", "") != "" else None
            trade_status_explain = row.get("tradeStatusExplain", None) if row.get("tradeStatusExplain", "") != "" else None
            shop_name = row.get("shopName", None) if row.get("shopName", "") != "" else None
            logistic_name = row.get("logisticName", None) if row.get("logisticName", "") != "" else None
            shop_id = row.get("shopId", None) if row.get("shopId", "") != "" else None
            payment = row.get("payment", None) if row.get("payment", "") != "" else None
            gmt_create = row.get("gmtCreate", None) if row.get("gmtCreate", "") != "" else None
            orderNo = row.get("orderNo", None) if row.get("orderNo", "") != "" else None
            
            last_ship_time = row.get("lastShipTime", None)
            if last_ship_time == "" or last_ship_time is None:
                last_ship_time = None
            else:
                try:
                    last_ship_time = datetime.strptime(last_ship_time, "%Y-%m-%d %H:%M:%S")
                except ValueError:
                    last_ship_time = None

            cursor.execute(insert_query, (
                trade_no, tradeTime, goodslist, trade_status, trade_status_explain, shop_name, logistic_name, shop_id, payment, gmt_create,
                flag_names, last_ship_time, orderNo
            ))
        connection.commit()
        cursor.close()
        connection.close()
        print(f"交易数据成功保存到数据库！")
    except Exception as e:
        print(f"保存交易数据到数据库时出错: {e}")
def save_to_db_stock(data):
    """将库存数据保存到MySQL数据库的stock_data表"""
    try:
        connection = pymysql.connect(
            host=DB_HOST,
            port=DB_PORT,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME
        )
        cursor = connection.cursor()
        insert_query = """
        INSERT INTO stock_quantity (
            quantityId, goodsId, goodsNo, goodsName, skuId, skuName, skuBarcode, unitName, currentQuantity, stockInQuantity
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE 
            quantityId = VALUES(quantityId), goodsNo = VALUES(goodsNo), goodsName = VALUES(goodsName);
        """

        for row in data:
            values = (
                row.get("quantityId"), 
                row.get("goodsId"), 
                row.get("goodsNo"), 
                row.get("goodsName"),
                row.get("skuId"),
                row.get("skuName"),
                row.get("skuBarcode"),
                row.get("unitName"),
                row.get("currentQuantity"),
                row.get("stockInQuantity")
            )
            cursor.execute(insert_query, values)
        connection.commit()
        cursor.close()
        connection.close()
        print(f"库存数据成功保存到数据库！")
    except Exception as e:
        print(f"保存库存数据到数据库时出错: {e}")

# ================== 数据转存 ===================
if __name__ == "__main__":
    all_trade_rows = []
    all_stock_rows = []
    page_size = 50
    today = datetime.today()
    tomorrow = today + timedelta(days=1)
    end_created = tomorrow.replace(minute=0, second=0, microsecond=0)
    start_created = (tomorrow - timedelta(days=35)).replace(hour=0, minute=0, second=0, microsecond=0)
    # 获取交易数据
    for i in range(5): 
        page = 0 
        window_start = start_created + timedelta(days=7 * i)  
        window_end = start_created + timedelta(days=7 * (i + 1))  
        window_start_str = window_start.strftime("%Y-%m-%d %H:%M:%S")
        window_end_str = window_end.strftime("%Y-%m-%d %H:%M:%S")
        print(f"请求交易时间窗口: {window_start_str} 到 {window_end_str}")
        while True:
            bizcontent_map = {
                "fields": "tradeNo,tradeStatus,tradeStatusExplain,shopName,logisticName,shopId,payment,gmtCreate,"
                          "flagNames,lastShipTime,tradeTime,goodslist,orderNo",  
                "pageindex": page,
                "pageSize": page_size,
                "startCreated": window_start_str,
                "endCreated": window_end_str
            }
            encoded_bizcontent = json.dumps(bizcontent_map)
            result = post("oms.trade.fullinfoget", "v1.0", encoded_bizcontent)
            if not result:
                break
            try:
                result_json = json.loads(result)
            except json.JSONDecodeError:
                break
            if not result_json or "result" not in result_json:
                break
            trades = result_json.get("result", {}).get("data", {}).get("trades", [])
            if not trades:
                break
            for trade in trades:
                base_info = {
                    "tradeNo": trade.get("tradeNo", ""),
                    "tradeStatus": trade.get("tradeStatus", ""),
                    "tradeStatusExplain": trade.get("tradeStatusExplain", ""),
                    "shopName": trade.get("shopName", ""),
                    "logisticName": trade.get("logisticName", ""),
                    "shopId": trade.get("shopId", ""),
                    "payment": trade.get("payment", ""),
                    "gmtCreate": trade.get("gmtCreate", ""),
                    "flagNames": trade.get("flagNames", ""),
                    "lastShipTime": trade.get("lastShipTime", ""),
                    "orderNo": trade.get("orderNo", ""),
                    "tradeTime": trade.get("tradeTime", ""),
                    "goodslist": trade.get("goodslist", "")
                }
                if base_info["tradeNo"] not in [row["tradeNo"] for row in all_trade_rows]:
                    all_trade_rows.append(base_info)
            page += 1
            if len(trades) < page_size:
                break
            time.sleep(0.5)
    if all_trade_rows:
        save_to_db_trade(all_trade_rows)
    else:
        print("没有获取到任何交易数据。")
    # 获取库存数据
    end_created = today.replace(hour=0, minute=0, second=0, microsecond=0)
    start_created = (today - timedelta(days=7)).replace(hour=0, minute=0, second=0, microsecond=0)
    start_created_str = start_created.strftime("%Y-%m-%d %H:%M:%S")
    end_created_str = end_created.strftime("%Y-%m-%d %H:%M:%S")
    print(f"请求库存时间窗口: {start_created_str} 到 {end_created_str}")
    bizcontent_map = {
        "fields": "stockInQuantity,quantityId,goodsId,goodsName,skuId,skuName,currentQuantity,skuBarcode,unitName",
        "pageindex": 0,
        "pageSize": 50,   # 只请求一次第一页的数据
        "startCreated": start_created_str,
        "endCreated": end_created_str
    }
    encoded_bizcontent = json.dumps(bizcontent_map)
    result = post("erp.stockquantity.get", "v1.0", encoded_bizcontent)
    if not result:
        print("请求失败，退出。")
    else:
        result_json = json.loads(result)
        goods_stock_quantity = result_json.get("result", {}).get("data", {}).get("goodsStockQuantity", [])

        if not goods_stock_quantity:
            print("没有获取到任何库存数据。")
        else:
            for good_stock_quantity in goods_stock_quantity:
                all_stock_rows.append(good_stock_quantity)
            # 保存数据
            save_to_db_stock(all_stock_rows)