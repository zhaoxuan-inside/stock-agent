#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
股票数据同步脚本 - PostgreSQL 18 版
API文档：https://www.zhituapi.com/hsstockapi.html
"""

import uuid
import requests
import logging
from datetime import datetime

# ---------- 配置区域 ----------
API_URL = "https://api.zhituapi.com/hs/list/all"
API_TOKEN = "89912D72-30F7-4A63-BFBA-DE39CDE31575"  # 请替换为您的真实 Token

# PostgreSQL 连接配置
PG_CONFIG = {
    "host": "192.168.2.112",
    "port": 5432,
    "user": "stock",
    "password": "iPasswd1234",
    "database": "stock",        # 如数据库名不是 stock 请修改
}
# ---------------------------

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def get_db_connection():
    """获取 PostgreSQL 连接"""
    import psycopg2
    return psycopg2.connect(**PG_CONFIG)


def init_database():
    """初始化表结构（自动创建表及触发器等）"""
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        # 建表语句：id 为 UUID，code 设唯一约束
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS stock_basic_info (
            id UUID PRIMARY KEY,
            code VARCHAR(20) NOT NULL UNIQUE,
            name VARCHAR(100) NOT NULL,
            exchanger VARCHAR(10) NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        cursor.execute(create_table_sql)

        # 创建自动更新 updated_at 的触发器函数
        create_function_sql = """
        CREATE OR REPLACE FUNCTION update_updated_at_column()
        RETURNS TRIGGER AS $$
        BEGIN
            NEW.updated_at = CURRENT_TIMESTAMP;
            RETURN NEW;
        END;
        $$ language 'plpgsql';
        """
        cursor.execute(create_function_sql)

        # 绑定触发器
        create_trigger_sql = """
        DROP TRIGGER IF EXISTS update_stock_basic_info_updated_at ON stock_basic_info;
        CREATE TRIGGER update_stock_basic_info_updated_at
            BEFORE UPDATE ON stock_basic_info
            FOR EACH ROW
            EXECUTE FUNCTION update_updated_at_column();
        """
        cursor.execute(create_trigger_sql)

        conn.commit()
        logger.info("数据库表 stock_basic_info 初始化成功（含唯一约束和更新时间触发器）")
    except Exception as e:
        logger.error(f"数据库初始化失败: {e}")
        raise
    finally:
        conn.close()


def fetch_stock_data():
    """从智兔 API 获取股票数据"""
    params = {"token": API_TOKEN}
    try:
        logger.info(f"正在请求 API: {API_URL}")
        response = requests.get(API_URL, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()
        logger.info(f"API 请求成功，获取到 {len(data)} 条记录")
        return data
    except requests.exceptions.RequestException as e:
        logger.error(f"API 请求失败: {e}")
        raise
    except ValueError as e:
        logger.error(f"JSON 解析失败: {e}")
        raise


def upsert_stock(conn, stock_item):
    """插入或更新单条股票记录（基于 code 唯一约束）"""
    code = stock_item.get("dm")
    name = stock_item.get("mc")
    exchanger = stock_item.get("jys")

    if not all([code, name, exchanger]):
        logger.warning(f"数据不完整，跳过: {stock_item}")
        return False

    try:
        cursor = conn.cursor()
        stock_id = str(uuid.uuid4())
        now = datetime.now()

        # ON CONFLICT (code) 处理重复：更新 name, exchanger, updated_at
        sql = """
        INSERT INTO stock_basic_info (id, code, name, exchanger, created_at, updated_at)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (code) DO UPDATE SET
            name = EXCLUDED.name,
            exchanger = EXCLUDED.exchanger,
            updated_at = EXCLUDED.updated_at
        """
        cursor.execute(sql, (stock_id, code, name, exchanger, now, now))
        conn.commit()
        logger.debug(f"成功处理股票: {code} - {name}")
        return True
    except Exception as e:
        logger.error(f"处理股票 {code} 失败: {e}")
        conn.rollback()
        return False


def save_to_database(stock_basic_info):
    """批量保存数据"""
    conn = get_db_connection()
    try:
        success = 0
        for stock in stock_basic_info:
            if upsert_stock(conn, stock):
                success += 1
        logger.info(f"数据处理完成，成功: {success}/{len(stock_basic_info)}")
    finally:
        conn.close()


def main():
    logger.info("=== 股票数据同步脚本启动（PostgreSQL）===")
    # 1. 初始化表结构
    init_database()
    # 2. 获取 API 数据
    stock_data = fetch_stock_data()
    if not stock_data:
        logger.warning("未获取到任何数据，脚本结束")
        return
    # 3. 写入数据库
    save_to_database(stock_data)
    logger.info("=== 脚本执行完成 ===")


if __name__ == "__main__":
    main()