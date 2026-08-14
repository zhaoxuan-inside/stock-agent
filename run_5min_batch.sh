#!/bin/bash
# 批量采集5分钟K线数据
# 每次从 exchange_date.log 取前7个日期，逐个执行完成后从原文件删除

set -euo pipefail

DATE_LOG="/home/hardstone/CodeSpace/stock-agent/exchange_date.log"
VENV_ACTIVATE="/home/hardstone/CodeSpace/stock-agent/.venv/bin/activate"
SCRIPT="/home/hardstone/CodeSpace/stock-agent/src/stock_exchange_5_min.py"
BATCH_SIZE=7

source "$VENV_ACTIVATE"

# 检查日志文件是否存在
if [[ ! -f "$DATE_LOG" ]]; then
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $DATE_LOG 不存在，退出。"
    exit 1
fi

# 读取前7个日期
mapfile -t TODO_DATES < <(head -n "$BATCH_SIZE" "$DATE_LOG" | sed '/^$/d')

if [[ ${#TODO_DATES[@]} -eq 0 ]]; then
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] 日期文件为空，全部已完成。"
    exit 0
fi

echo "[$(date '+%Y-%m-%d %H:%M:%S')] 本次处理 ${#TODO_DATES[@]} 个日期: ${TODO_DATES[*]}"

FAIL_COUNT=0
for date in "${TODO_DATES[@]}"; do
    echo "============================================"
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] 开始处理: $date"
    echo "============================================"

    if python "$SCRIPT" --date "$date"; then
	echo "finish ${date} data grapi..."
	sed -i "/${date}$/d" exchange_date.log
        echo "[$(date '+%Y-%m-%d %H:%M:%S')] $date 完成 ✓"
    else
        echo "[$(date '+%Y-%m-%d %H:%M:%S')] $date 失败 ✗"
        FAIL_COUNT=$((FAIL_COUNT + 1))
    fi
done
