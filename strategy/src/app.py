#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
strategy/src/app.py

职责：
  - 作为“一个交易对一个实例”的示例策略进程
  - 从 Redis Stream 读取 data-fetcher 写入的 ticker 事件（stream）
  - 维护短期价格窗口，计算简单移动平均（SMA）
  - 基于 SMA 短/长交叉输出信号（打印），并把信号写回 Redis 的 signals stream（便于后续消费）
  - 不做任何实盘下单，仅打印模拟动作（便于测试）
说明：
  - 使用 Consumer Group（XGROUP / XREADGROUP），保证消息被确认/可回放
  - 通过环境变量配置 exchange/symbol/stream key 等
"""

# ======= 标准库导入 =======
import os                            # 读取环境变量
import time                          # sleep / 时间控制
import json                          # 序列化 / 反序列化消息
import logging                       # 统一日志接口（替代 print）
from collections import deque        # 高效固定长度队列用于滑动窗口
from typing import Deque, Dict, Any  # 类型注释（可读性帮助）

# ======= 第三方库导入 =======
from dotenv import load_dotenv       # 读取 .env 文件
import redis                         # Redis 客户端（redis-py）

# 载入 .env（如果存在则覆盖环境变量）
load_dotenv()

# ------------------- 配置（从 .env 或默认） -------------------
# 交易所名（仅用于日志/键名）
EXCHANGE = os.getenv("STRAT_EXCHANGE", os.getenv("EXCHANGES", "binance").split(",")[0])
# 交易对（比如 "BTC/USDT"）
SYMBOL = os.getenv("STRAT_SYMBOL", "BTC/USDT")
# timeframe（与 data-fetcher 保持一致）
TIMEFRAME = os.getenv("TIMEFRAME", "1m")
# redis 连接参数
REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_DB = int(os.getenv("REDIS_DB", "0"))
# consumer group / consumer 名字（多个实例应不同 consumer 名以示区分）
GROUP_NAME = os.getenv("STRAT_GROUP", f"grp:{EXCHANGE}:{SYMBOL.replace('/','').lower()}:{TIMEFRAME}")
CONSUMER_NAME = os.getenv("STRAT_CONSUMER", f"consumer:{os.getenv('HOSTNAME','local')}")
# stream 名称（与 data-fetcher 的 stream 命名约定一致）
STREAM_KEY = os.getenv("STRAT_STREAM_KEY", f"stream:ticker:{EXCHANGE}:{SYMBOL.replace('/','').lower()}:{TIMEFRAME}")
# signals stream（策略产生信号后写入，便于监控或下游消费）
SIGNAL_STREAM = os.getenv("STRAT_SIGNAL_STREAM", f"stream:signals:{EXCHANGE}:{SYMBOL.replace('/','').lower()}:{TIMEFRAME}")

# 策略参数（短/长均线窗口，可通过 .env 调整）
MA_SHORT = int(os.getenv("MA_SHORT", "5"))   # 短均线窗口（单位为消息数）
MA_LONG  = int(os.getenv("MA_LONG", "20"))   # 长均线窗口（单位为消息数）

# XREADGROUP 阻塞等待时间（毫秒）
XREADGROUP_BLOCK_MS = int(os.getenv("XREADGROUP_BLOCK_MS", "2000"))

# 连接重试/等待（秒）
RECONNECT_WAIT = float(os.getenv("RECONNECT_WAIT", "2.0"))

# ======= 日志配置（统一输出到 stdout，docker-compose 能捕获） =======
# level 可改为 logging.DEBUG 便于调试
logging.basicConfig(
  level=logging.INFO,
  format="%(asctime)s [%(levelname)s] %(message)s",
  handlers=[logging.StreamHandler()]  # 输出到 stdout
)
logger = logging.getLogger("strategy")

# ------------------- 初始化 Redis 客户端 -------------------
# decode_responses=True 让 redis 返回 str 而不是 bytes，方便 json.loads 直接用
redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, decode_responses=True)

# ------------------- 简单状态与窗口 -------------------
# 使用 deque 保存最新 N 个价格，方便计算 SMA
prices: Deque[float] = deque(maxlen=MA_LONG)  # 保存到 MA_LONG 长度
last_signal = None  # 记录上一次信号，避免重复打印相同信号

# ------------------- 辅助函数 -------------------
def ensure_consumer_group(stream: str, group: str) -> None:
  """
  确保指定的 consumer group 存在（若不存在则创建）。
  参数:
    - stream: Redis stream key 名称
    - group: consumer group 名称
  行为:
    - 若 stream 不存在则使用 mkstream=True 创建
    - 若 group 已存在则忽略 BUSYGROUP 错误
  """
  try:
    # 创建 consumer group，从 0 开始（或用 '$' 仅消费新消息）
    redis_client.xgroup_create(stream, group, id='0', mkstream=True)
    logger.info("创建 consumer group 成功 stream=%s group=%s", stream, group)
  except redis.exceptions.ResponseError as e:
    # 如果组已经存在，Redis 会报 BUSYGROUP，忽略该错误
    if "BUSYGROUP" in str(e):
      logger.debug("consumer group 已存在 stream=%s group=%s", stream, group)
      return
    # 其它错误需要抛出以便排查
    logger.exception("xgroup_create 失败: %s", e)
    raise

def sma(window: Deque[float]) -> float:
  """
  计算简单移动平均（SMA），若窗口为空返回 0.0
  参数:
    - window: deque 或者可迭代的价格序列
  返回:
    - float SMA
  """
  if not window:
    return 0.0
  return sum(window) / len(window)

def process_message(msg_id: str, msg_fields: Dict[str, Any]) -> None:
  """
  处理单条来自 stream 的消息：
    - msg_id: Redis stream 消息 ID（如 '163783:0'）
    - msg_fields: Redis 返回的 field map，例如 {"data": "<json string>"}
  行为:
    - 解析消息 payload，提取价格，更新滑动窗口
    - 计算短/长均线，判断是否产生 BUY/SELL 信号
    - 若产生信号，XADD 到 SIGNAL_STREAM，并记录日志
  """
  global last_signal
  try:
    # data-fetcher 写入 stream 时约定把 JSON 放到字段 "data"
    # 兼容地取可能的字段名（"data" 或 "payload" 或取第一个 field）
    raw = msg_fields.get("data") or msg_fields.get("payload") or next(iter(msg_fields.values()), None)
    if raw is None:
      logger.warning("空消息字段，msg_id=%s, fields=%s", msg_id, msg_fields)
      return

    # 解析 JSON 字符串为 dict
    payload = json.loads(raw)

    # payload 约定包含 "last" 或 "price" 字段（data-fetcher 中我们用 "last"）
    price = payload.get("last") or payload.get("price")
    if price is None:
      logger.debug("消息中没有 price 字段，跳过 msg_id=%s payload=%s", msg_id, payload)
      return

    # 强制转成 float（谨防字符串）
    price_f = float(price)

    # 把价格追加到窗口（deque 自动丢弃最旧元素）
    prices.append(price_f)

    # 计算短/长均线（当价格数够时）
    short_ma = sma(list(prices)[-MA_SHORT:]) if len(prices) >= 1 else 0.0
    long_ma  = sma(prices) if len(prices) >= 1 else 0.0

    # 简单交叉逻辑：短线上穿长线 => BUY，短线下穿长线 => SELL
    signal = None
    if len(prices) >= MA_SHORT and len(prices) >= MA_LONG:
      # 只有当窗口足够大才判定交叉（避免过早发信）
      if short_ma > long_ma and last_signal != "BUY":
        signal = "BUY"
      elif short_ma < long_ma and last_signal != "SELL":
        signal = "SELL"

    # 记录决策日志（INFO 级别）
    logger.info("[策略][%s %s] price=%.6f short_ma=%.6f long_ma=%.6f signal=%s",
                EXCHANGE, SYMBOL, price_f, short_ma, long_ma, signal)

    # 若有信号，则写入 signals stream（便于监控/下游处理）
    if signal:
      sig_payload = {
        "exchange": EXCHANGE,
        "symbol": SYMBOL,
        "time": payload.get("timestamp"),
        "price": price_f,
        "signal": signal,
        "short_ma": short_ma,
        "long_ma": long_ma
      }
      # XADD 到 SIGNAL_STREAM（保留最近 10000 条，近似修剪）
      #redis_client.xadd(SIGNAL_STREAM, {"data": json.dumps(sig_payload)}, maxlen=10000, approximate=True)
      logger.info("[策略][%s %s] 模拟动作 => %s at %.6f (signal 写入 %s)",
                  EXCHANGE, SYMBOL, signal, price_f, SIGNAL_STREAM)
      last_signal = signal

  except Exception as e:
    # 捕捉并记录异常，策略不应因为单条消息错误崩掉进程
    logger.exception("处理消息异常 msg_id=%s error=%s", msg_id, e)

# ------------------- 主循环 -------------------
def run():
  """
  主运行函数：
    - 确保 consumer group 存在
    - 循环使用 XREADGROUP 阻塞读取新消息（'>'），处理并 XACK
  注意：
    - XREADGROUP(..., {STREAM_KEY: '>'}) 读取未被任何 consumer 处理的新消息
    - 若要处理 pending（已交付但未 ACK）消息，需要额外实现 XPENDING/XCLAIM 逻辑
  """
  # 确保组存在（若已存在则忽略错误）
  ensure_consumer_group(STREAM_KEY, GROUP_NAME)
  logger.info("启动 consumer group=%s consumer=%s stream=%s", GROUP_NAME, CONSUMER_NAME, STREAM_KEY)

  # 主循环：持续从 stream 读取并处理
  while True:
    try:
      # XREADGROUP 从 consumer group 中读取新消息（'>'），阻塞 XREADGROUP_BLOCK_MS 毫秒
      # 返回结构: [(stream_key, [(msg_id, {field: value}), ...]), ...]
      entries = redis_client.xreadgroup(GROUP_NAME, CONSUMER_NAME, {STREAM_KEY: ">"}, count=1, block=XREADGROUP_BLOCK_MS)
      if not entries:
        # 阻塞超时未收到新消息，回到循环继续等待
        continue

      # 遍历读取到的消息（通常只订阅一个 stream）
      for stream_name, msgs in entries:
        for msg_id, msg_fields in msgs:
          # 处理消息（解析、计算、可能写 signal）
          process_message(msg_id, msg_fields)
          # ACK 表示该消息已被本 consumer 成功处理，后续不会被同组再发
          redis_client.xack(STREAM_KEY, GROUP_NAME, msg_id)

    except redis.exceptions.ConnectionError as e:
      # Redis 连接断开：记录错误并短暂等待后重试连接
      logger.error("Redis 连接错误: %s，%ss 后重连", e, RECONNECT_WAIT)
      time.sleep(RECONNECT_WAIT)
      continue
    except KeyboardInterrupt:
      # 人工中断，优雅退出
      logger.info("收到 KeyboardInterrupt，退出")
      break
    except Exception as e:
      # 其它未知异常：记录并短暂休眠后继续（防止快速失败循环）
      logger.exception("主循环未捕获异常: %s", e)
      time.sleep(1.0)
      continue

# ------------------- 程序入口 -------------------
if __name__ == "__main__":
  run()
