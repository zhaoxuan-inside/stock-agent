#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
股票历史5分钟线数据采集脚本（BaoStock -> PostgreSQL）
- 分页读取股票代码
- 多进程 + 强制超时（单只股票绝对超时）
- 独立登录，避免全局阻塞
- 自动重试，跳过失败股票
- Upsert（插入或更新）策略
- 断点续传：记录进度，中断后可从断点继续
"""

import sys
import logging
import time
import os
from datetime import datetime
from typing import List
import multiprocessing as mp

import psycopg2
from psycopg2.extras import execute_values
import baostock as bs

# ====================== 配置区域 ======================
DB_CONFIG = {
    "host": "192.168.2.112",
    "port": 5432,
    "database": "stock",      # 请修改
    "user": "stock",          # 请修改
    "password": "iPasswd1234"  # 请修改
}

#START_DATE = datetime.now().strftime("%Y-%m-%d")
START_DATE = "2013-01-04" 
END_DATE = datetime.now().strftime("%Y-%m-%d")
BATCH_SIZE = 50                  # 每批处理的股票数量
PROCESS_TIMEOUT = 180             # 单只股票处理超时（秒）
MAX_RETRIES = 2                  # 每只股票最大重试次数
RETRY_DELAY = 5                  # 重试间隔（秒）
UPSERT_PAGE_SIZE = 1000          # 单只股票内部批量插入大小

PROGRESS_FILE = "5_min_stock_fetch_progress.txt"  # 记录进度的文件
# ====================================================

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

# Baostock 原始字段列表（不含 code）
BAOSTOCK_FIELDS = [
    "date", "time", "code", "open", "high", "low", "close",
    "volume", "amount", "adjustflag"
]

FIELD_MAPPING = {
    "adjustflag": "adjust_flag"
}

DB_COLUMNS = [
    "date", "code", "time", "open", "high", "low", "close",
    "volume", "amount", "adjust_flag"
]


def get_db_connection():
    return psycopg2.connect(**DB_CONFIG)


def get_total_stock_count(conn) -> int:
    """获取股票总数"""
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM stock_basic_info")
        return cur.fetchone()[0]


def get_stock_codes_paginated(conn, offset: int, limit: int) -> List[str]:
    """分页获取股票代码（按 code 升序）"""
    with conn.cursor() as cur:
        cur.execute("SELECT code FROM stock_basic_info ORDER BY code LIMIT %s OFFSET %s", (limit, offset))
        rows = cur.fetchall()
    return [row[0] for row in rows]


def load_progress() -> int:
    """
    从本地文件读取上次处理到的偏移量（已完成股票数量）。
    如果文件不存在，返回 0。
    """
    if not os.path.exists(PROGRESS_FILE):
        return 0
    try:
        with open(PROGRESS_FILE, 'r') as f:
            offset = int(f.read().strip())
            logger.info("读取进度文件：已完成 %d 只股票", offset)
            return offset
    except Exception as e:
        logger.warning("读取进度文件失败: %s，将从头开始", e)
        return 0


def save_progress(offset: int):
    """保存当前偏移量到本地文件"""
    try:
        with open(PROGRESS_FILE, 'w') as f:
            f.write(str(offset))
        logger.debug("保存进度: offset=%d", offset)
    except Exception as e:
        logger.warning("保存进度文件失败: %s", e)


def convert_row_to_db_types(row: dict, stock_code: str) -> dict:
    converted = {}
    for key, value in row.items():
        db_key = FIELD_MAPPING.get(key, key)
        if value == '' or value is None:
            converted[db_key] = None
            continue
        if db_key == 'volume':
            converted[db_key] = int(float(value))
        elif db_key == 'date':
            converted[db_key] = value
        else:
            try:
                converted[db_key] = float(value)
            except (ValueError, TypeError):
                converted[db_key] = None
    converted['code'] = stock_code
    return converted


def process_one_stock(stock_code: str, start_date: str, end_date: str) -> tuple:
    """
    处理单只股票：获取数据并写入数据库。
    返回值 (stock_code, success, record_count, error_msg)
    """
    # 每个进程独立登录
    login_result = bs.login()
    if login_result.error_code != '0':
        return (stock_code, False, 0, f"登录失败: {login_result.error_msg}")

    try:
        rs = bs.query_history_k_data_plus(
            code=stock_code,
            fields=','.join(BAOSTOCK_FIELDS),
            start_date=start_date,
            end_date=end_date,
            frequency="5",
            adjustflag="3"
        )
        if rs.error_code != '0':
            return (stock_code, False, 0, f"查询失败: {rs.error_msg}")

        records = []
        while rs.next():
            raw_row = dict(zip(BAOSTOCK_FIELDS, rs.get_row_data()))
            db_row = convert_row_to_db_types(raw_row, stock_code)
            records.append(db_row)

        if not records:
            return (stock_code, True, 0, "无数据")

        # 写入数据库
        conn = get_db_connection()
        try:
            values = [tuple(record.get(col) for col in DB_COLUMNS) for record in records]
            update_columns = [col for col in DB_COLUMNS if col not in ('date', 'code')]
            update_set = ', '.join([f"{col} = EXCLUDED.{col}" for col in update_columns])
            update_set += ", updated_at = NOW()"
            upsert_sql = f"""
                INSERT INTO stock_exchange_5_min ({', '.join(DB_COLUMNS)})
                VALUES %s
                ON CONFLICT (date, time, code) DO UPDATE SET {update_set}
            """
            with conn.cursor() as cur:
                execute_values(cur, upsert_sql, values, page_size=UPSERT_PAGE_SIZE)
            conn.commit()
            return (stock_code, True, len(records), None)
        finally:
            conn.close()
    except Exception as e:
        return (stock_code, False, 0, str(e))
    finally:
        bs.logout()


def run_with_timeout(stock_code: str, start_date: str, end_date: str, timeout: int) -> tuple:
    """
    在独立进程中运行 process_one_stock，并设置超时。
    """
    ctx = mp.get_context('fork')
    q = ctx.Queue()
    p = ctx.Process(target=lambda q, args: q.put(process_one_stock(*args)), args=(q, (stock_code, start_date, end_date)))
    p.start()
    p.join(timeout)

    if p.is_alive():
        p.terminate()
        p.join()
        return (stock_code, False, 0, f"超时（>{timeout}秒）")
    else:
        return q.get()


def main():
    start_time = datetime.now()
    logger.info("===== 开始采集股票日线数据（断点续传模式） =====")

    # 1. 连接数据库获取信息
    conn = get_db_connection()
    try:
        total_stocks = get_total_stock_count(conn)
        logger.info("数据库中共有 %d 只股票", total_stocks)
    finally:
        conn.close()

    # 2. 读取上次进度
    start_offset = load_progress()
    if start_offset >= total_stocks:
        logger.info("所有股票已处理完成，无需继续。")
        return

    logger.info("从偏移量 %d 开始继续处理（已跳过 %d 只股票）", start_offset, start_offset)

    offset = start_offset
    success_count = 0
    fail_count = 0
    total_records = 0
    processed_count = 0  # 当前会话已处理股票数（用于日志）

    while offset < total_stocks:
        # 分批获取股票代码
        conn = get_db_connection()
        try:
            batch_codes = get_stock_codes_paginated(conn, offset, BATCH_SIZE)
        finally:
            conn.close()

        if not batch_codes:
            break

        logger.info("处理第 %d - %d 批股票，共 %d 只", offset+1, offset+len(batch_codes), len(batch_codes))

        # 处理当前批次的每只股票
        for code in batch_codes:
            # 重试机制
            for attempt in range(1, MAX_RETRIES + 1):
                logger.info("处理股票 %s (尝试 %d/%d)", code, attempt, MAX_RETRIES)
                result = run_with_timeout(code, START_DATE, END_DATE, PROCESS_TIMEOUT)
                stock, ok, rec_count, err_msg = result
                if ok:
                    success_count += 1
                    processed_count += 1
                    total_records += rec_count
                    logger.info("股票 %s 成功，获取 %d 条记录", stock, rec_count)
                    break
                else:
                    logger.error("股票 %s 失败 (尝试 %d/%d): %s", stock, attempt, MAX_RETRIES, err_msg)
                    if attempt < MAX_RETRIES:
                        time.sleep(RETRY_DELAY)
                    else:
                        fail_count += 1
                        processed_count += 1
                        logger.error("股票 %s 最终失败，跳过", stock)

        # 更新偏移量：当前批次处理完后，offset 增加 batch 大小
        offset += len(batch_codes)
        # 保存进度（每批完成后保存一次）
        save_progress(offset)
        logger.info("当前进度: 成功 %d, 失败 %d, 总计记录 %d, 已处理 %d/%d 只股票",
                    success_count, fail_count, total_records, offset, total_stocks)

    # 最终完成，删除进度文件（可选）
    if offset >= total_stocks:
        logger.info("所有股票处理完毕，删除进度文件")
        if os.path.exists(PROGRESS_FILE):
            os.remove(PROGRESS_FILE)

    elapsed = datetime.now() - start_time
    logger.info("===== 采集完成 =====")
    logger.info("成功: %d, 失败: %d, 总记录数: %d, 耗时: %s", success_count, fail_count, total_records, elapsed)


if __name__ == "__main__":
    mp.set_start_method('fork', force=True)
    main()
