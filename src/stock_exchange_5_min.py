#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
股票历史5min数据采集脚本（BaoStock -> PostgreSQL）
优化版：单进程串行 + 全局登录一次 + signal超时控制
- 分页读取股票代码
- 全程只登录一次，避免频繁登录
- 单只股票独立超时（signal.alarm）
- 自动重试，跳过失败股票
- Upsert（插入或更新）策略
- 断点续传：记录进度，中断后可从断点继续
"""

import argparse
import sys
import logging
import time
import os
import signal
from datetime import datetime
from typing import List

import psycopg2
from psycopg2.extras import execute_values
import baostock as bs

# ====================== 配置区域 ======================
DB_CONFIG = {
    "host": "192.168.2.112",
    "port": 5432,
    "database": "stock",          # 请修改
    "user": "stock",              # 请修改
    "password": "iPasswd1234"     # 请修改
}

START_DATE = datetime.now().strftime("%Y-%m-%d")
END_DATE = datetime.now().strftime("%Y-%m-%d")
BATCH_SIZE = 50                  # 每批获取的股票数量（仅用于分页读取代码）
SINGLE_STOCK_TIMEOUT = 1200       # 单只股票处理超时（秒）
MAX_RETRIES = 5                  # 每只股票最大重试次数
RETRY_DELAY = 60                  # 重试间隔（秒）
UPSERT_PAGE_SIZE = 1000          # 单只股票内部批量插入大小

PROGRESS_FILE = "stock_5_minfetch_progress.txt"  # 记录进度的文件
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
    "date", "time", "code", "open", "high", "low", "close",
    "volume", "amount", "adjust_flag"
]


def get_db_connection():
    """创建 PostgreSQL 连接"""
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
    """从本地文件读取上次处理到的偏移量（已完成股票数量）"""
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
    """将 baostock 原始数据行转换为数据库字段格式"""
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
    处理单只股票的核心逻辑（baostock 已全局登录，无需重复登录）
    返回值 (stock_code, success, record_count, error_msg)
    """
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
            if rs.error_code == '10001001':
                bs.logout()  # 可能是登录状态异常，先登出
                time.sleep(10)  # 等待一会儿再继续
                bs.login()   # 重新登录
                return process_one_stock(stock_code, start_date, end_date)  # 重试一次
            return (stock_code, False, 0, f"查询失败: code: {rs.error_code} msg: {rs.error_msg}")

        records = []
        while rs.next():
            raw_row = dict(zip(BAOSTOCK_FIELDS, rs.get_row_data()))
            db_row = convert_row_to_db_types(raw_row, stock_code)
            records.append(db_row)

        if not records:
            return (stock_code, True, 0, None)

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


def handle_stock_with_timeout(stock_code: str, start_date: str, end_date: str, timeout: int) -> tuple:
    """
    为单只股票设置超时控制（使用 signal.alarm）
    返回 (stock_code, success, record_count, error_msg)
    """
    def _timeout_handler(signum, frame):
        raise TimeoutError(f"股票处理超时（>{timeout}秒）")

    # 保存原有信号处理器，处理完后恢复
    original_handler = signal.signal(signal.SIGALRM, _timeout_handler)
    signal.alarm(timeout)

    try:
        result = process_one_stock(stock_code, start_date, end_date)
        signal.alarm(0)  # 取消闹钟
        return result
    except TimeoutError as e:
        signal.alarm(0)
        return (stock_code, False, 0, str(e))
    finally:
        signal.signal(signal.SIGALRM, original_handler)


def main():

    parser = argparse.ArgumentParser(description='脚本说明：根据状态日期执行任务')
    parser.add_argument('--date', type=str, required=False, help='状态日期，格式 YYYY-MM-DD，用于指导脚本执行')
    args = parser.parse_args()
    date = args.date

    start_time = datetime.now()

    START_DATE = start_time.strftime("%Y-%m-%d")
    if date:
        START_DATE = date
    END_DATE = START_DATE

    logger.info("[%s] 采集 %s 5分钟k线数据", start_time, START_DATE)

    # 1. 全局登录 baostock（只登录一次）
    login_result = bs.login()
    if login_result.error_code != '0':
        logger.error("baostock 登录失败: %s，程序退出", login_result.error_msg)
        sys.exit(1)
    logger.info("baostock 全局登录成功")

    # 2. 连接数据库获取股票列表信息
    conn = get_db_connection()
    try:
        total_stocks = get_total_stock_count(conn)
        logger.info("数据库中共有 %d 只股票", total_stocks)
    finally:
        conn.close()

    # 3. 读取上次进度
    start_offset = load_progress()
    if start_offset >= total_stocks:
        logger.info("所有股票已处理完成，无需继续。")
        bs.logout()
        return

    logger.info("从偏移量 %d 开始继续处理（已跳过 %d 只股票）", start_offset, start_offset)

    offset = start_offset
    success_count = 0
    fail_count = 0
    total_records = 0

    # 4. 循环分批获取股票代码并处理
    while offset < total_stocks:
        conn = get_db_connection()
        try:
            batch_codes = get_stock_codes_paginated(conn, offset, BATCH_SIZE)
        finally:
            conn.close()

        if not batch_codes:
            break

        logger.info("处理第 %d - %d 批股票，共 %d 只", offset+1, offset+len(batch_codes), len(batch_codes))

        # 串行处理当前批次的每只股票
        for code in batch_codes:
            # 重试机制
            final_success = False
            final_records = 0
            final_error = None

            for attempt in range(1, MAX_RETRIES + 1):
                logger.info("处理股票 %s (尝试 %d/%d)", code, attempt, MAX_RETRIES)
                result = handle_stock_with_timeout(code, START_DATE, END_DATE, SINGLE_STOCK_TIMEOUT)
                stock, ok, rec_count, err_msg = result

                if ok:
                    final_success = True
                    final_records = rec_count
                    final_error = None
                    break
                else:
                    final_error = err_msg
                    logger.error("股票 %s 失败 (尝试 %d/%d): %s", stock, attempt, MAX_RETRIES, err_msg)
                    if attempt < MAX_RETRIES:
                        time.sleep(RETRY_DELAY)

            if final_success:
                success_count += 1
                total_records += final_records
                logger.info("股票 %s 成功，获取 %d 条记录", code, final_records)
            else:
                fail_count += 1
                logger.error("股票 %s 最终失败，跳过: %s", code, final_error)

        # 更新偏移量（当前批次处理完）
        offset += len(batch_codes)
        save_progress(offset)
        logger.info("当前进度: 成功 %d, 失败 %d, 总计记录 %d, 已处理 %d/%d 只股票",
                    success_count, fail_count, total_records, offset, total_stocks)

    # 5. 全部处理完成，注销 baostock
    bs.logout()
    logger.info("baostock 已注销")

    # 删除进度文件
    if offset >= total_stocks:
        logger.info("所有股票处理完毕，删除进度文件")
        if os.path.exists(PROGRESS_FILE):
            os.remove(PROGRESS_FILE)

    elapsed = datetime.now() - start_time
    logger.info("===== 采集完成 =====")
    logger.info("成功: %d, 失败: %d, 总记录数: %d, 耗时: %s", success_count, fail_count, total_records, elapsed)


if __name__ == "__main__":
    main()
