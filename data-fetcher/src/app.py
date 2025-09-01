#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
data-fetcher/src/app.py

职责 summary:
  - 高频：定期拉取 ticker（用于实时监控/实时队列），写入 Redis（JSON）
  - 周期/补盘：按策略周期小批量拉取 OHLCV（例如 10 条），批量写入 Postgres (TimescaleDB)
  - 采用 .env 配置，默认使用正式环境（spot）
  - 提供注释说明如何开启 testnet / sandbox（开发时可选）

设计要点:
  - 将 ticker 与 OHLCV 分离：ticker 高频、OHLCV 批量/周期性补齐
  - 使用内存记录 last_insert_ts 避免大量重复写入
  - 使用 execute_values 批量写入提高效率
  - 对网络/交易所调用使用重试 + 指数退避
"""

import os                                       # 读取环境变量
import time                                     # sleep / 计时
import logging                                  # 日志记录
import signal                                   # 捕获 SIGINT/SIGTERM
import sys                                      # stdout
import json                                     # Redis 存储 JSON
from datetime import datetime                   # 时间处理（UTC）
from typing import Any, Dict, List, Optional    # 类型注解

from dotenv import load_dotenv                  # 加载 .env 文件
import ccxt                                     # 统一交易所接口
import redis                                    # Redis 客户端
import psycopg2                                 # Postgres 客户端
from psycopg2.extras import execute_values      # 高效批量插入

# ---------------- Load configuration from .env ----------------
load_dotenv()  # load environment variables from .env into os.environ

# Exchanges (comma separated), e.g. "binance,okx"
EXCHANGES = [s.strip() for s in os.getenv("EXCHANGES", "binance").split(",") if s.strip()]

# Symbols (comma separated), e.g. "BTC/USDT,ETH/USDT"
SYMBOLS = [s.strip() for s in os.getenv("SYMBOLS", "BTC/USDT").split(",") if s.strip()]

# Fetch interval in seconds for main loop (ticker frequency)
FETCH_INTERVAL = int(os.getenv("FETCH_INTERVAL", "5"))

# Timeframe used when fetching OHLCV, default "1m"
TIMEFRAME = os.getenv("TIMEFRAME", "1m")

# How many OHLCV rows to fetch in a batch when doing periodic OHLCV fetch
BATCH_OHLCV_LIMIT = int(os.getenv("BATCH_OHLCV_LIMIT", "10"))

# How many main-loop iterations between OHLCV batch fetches
OHLCV_EVERY_LOOPS = int(os.getenv("OHLCV_EVERY_LOOPS", "12"))

# Postgres (TimescaleDB) connection settings
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "db")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
POSTGRES_DB = os.getenv("POSTGRES_DB", "quant")
POSTGRES_USER = os.getenv("POSTGRES_USER", "quant")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "")

# Redis settings
REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_DB = int(os.getenv("REDIS_DB", "0"))

# Retry controls
DEFAULT_MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))
RETRY_BACKOFF_BASE = float(os.getenv("RETRY_BACKOFF_BASE", "1.5"))

# ---------------- Logging ----------------
logging.basicConfig(
  level=logging.INFO,  # 信息级别（可改为 DEBUG 进行更详细排查）
  format="%(asctime)s [%(levelname)s] %(message)s",
  stream=sys.stdout
)

# Running flag for graceful shutdown
running = True

# ---------------- Signal handling ----------------
def handle_signal(signum, frame):
  """
  信号处理：收到 SIGINT 或 SIGTERM 时，将全局 running 置 False
  这样主循环会优雅退出
  """
  global running
  logging.info("收到信号 %s，准备优雅退出...", signum)
  running = False

# 注册信号回调
signal.signal(signal.SIGINT, handle_signal)
signal.signal(signal.SIGTERM, handle_signal)

# ---------------- Utility helpers ----------------
def safe_get(d: Any, key: str, default: Any = None) -> Any:
  """
  从多种交易所返回结构中安全获取字段，避免因结构差异导致 KeyError。
  若 d 是 dict，返回 d.get(key, default)，否则返回 default。
  """
  try:
    if d is None:
      return default
    if isinstance(d, dict):
      return d.get(key, default)
  except Exception:
    pass
  return default

def parse_ohlcv_row(row: Any) -> Optional[Dict[str, Any]]:
  """
  将 ccxt fetch_ohlcv 返回的单行（[ts, o, h, l, c, v]）解析为 dict：
    { "timestamp": datetime, "open": float, "high": float, "low": float, "close": float, "volume": float }
  返回 None 表示解析失败（例如数据不完整）
  """
  try:
    if not row or len(row) < 6:
      return None
    ts_ms = int(row[0])                 # 毫秒时间戳
    dt = datetime.utcfromtimestamp(ts_ms / 1000.0)  # 转为 UTC datetime
    o = float(row[1])
    h = float(row[2])
    l = float(row[3])
    c = float(row[4])
    v = float(row[5])
    return {
      "timestamp": dt,
      "open": o,
      "high": h,
      "low": l,
      "close": c,
      "volume": v
    }
  except Exception:
    return None

# ---------------- Wait for dependent services ----------------
def wait_for_postgres(timeout: int = 60) -> bool:
  """
  尝试连接 Postgres 直到可用或超时。
  返回 True 表示可用，False 表示超时失败。
  """
  start = time.time()
  while time.time() - start < timeout and running:
    try:
      # 快速建立连接并关闭以测试可用性
      conn = psycopg2.connect(
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        connect_timeout=5
      )
      conn.close()  # 连接成功就关闭
      logging.info("Postgres 已就绪")
      return True
    except Exception as e:
      logging.info("等待 Postgres 可用... (%s)", e)
      time.sleep(2)
  logging.error("等待 Postgres 超时 (%s 秒)", timeout)
  return False

def wait_for_redis(timeout: int = 60) -> bool:
  """
  尝试连接 Redis 直到可用或超时。
  返回 True 表示可用，False 表示超时失败。
  """
  start = time.time()
  while time.time() - start < timeout and running:
    try:
      r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, socket_connect_timeout=5)
      if r.ping():                        # 使用 ping 验证可达性
        logging.info("Redis 已就绪")
        return True
    except Exception as e:
      logging.info("等待 Redis 可用... (%s)", e)
      time.sleep(2)
  logging.error("等待 Redis 超时 (%s 秒)", timeout)
  return False

# ---------------- Build ccxt exchange clients ----------------
def build_exchanges() -> Dict[str, ccxt.Exchange]:
  """
  根据 EXCHANGES 列表创建 ccxt 的交易所实例字典。
  - 如果环境变量中存在 {EXNAME}_API_KEY/_API_SECRET/_API_PASSWORD，则注入用于后续下单（但当前默认只读行情）
  - 强制 options.defaultType = 'spot'（以避免某些实现默认查询合约）
  返回: dict mapping exchange name -> ccxt exchange instance
  """
  ex_instances: Dict[str, ccxt.Exchange] = {}
  for ex_name in EXCHANGES:
    try:
      cls = getattr(ccxt, ex_name)     # 动态获取交易所类（如 ccxt.binance）
    except AttributeError:
      logging.error("ccxt 不支持交易所: %s，跳过", ex_name)
      continue

    try:
      # 读取可能存在的 API 密钥（仅在需要下单时启用）
      api_key = os.getenv(f"{ex_name.upper()}_API_KEY")
      api_secret = os.getenv(f"{ex_name.upper()}_API_SECRET")
      api_pass = os.getenv(f"{ex_name.upper()}_API_PASSWORD")  # OKX 可能需要 password 字段

      # 基本参数：启用速率限制 + 超时
      params: Dict[str, Any] = {"enableRateLimit": True, "timeout": 20000}
      params.setdefault("options", {})
      params["options"].setdefault("defaultType", "spot")  # 强制 spot

      # 若提供密钥则注入（谨慎：拥有写权限即等同 root 权限）
      if api_key and api_secret:
        params["apiKey"] = api_key
        params["secret"] = api_secret
      if api_pass:
        params["password"] = api_pass

      # 创建实例
      ex = cls(params)

      # 可选：手动进入 sandbox/testnet 模式（开发时开启）
      # WARNING: 一些交易所的 testnet 不提供全部公共接口（会导致 404）
      # 若要启用 testnet，可以取消下面注释（按需）：
      # ex.set_sandbox_mode(True)

      ex_instances[ex_name] = ex
      logging.info("创建 ccxt 客户端: %s", ex_name)
    except Exception as e:
      logging.exception("创建 %s 客户端失败: %s", ex_name, e)
  return ex_instances

# ---------------- Fetch & storage helpers ----------------
def fetch_and_store_ohlcv_batch(
  ex: ccxt.Exchange,
  ex_name: str,
  symbol: str,
  limit: int,
  pg_cur,
  pg_conn,
  last_ts_map: Dict[str, Dict[str, Optional[datetime]]]
) -> None:
  """
  批量拉取 OHLCV（limit 条），解析后批量写入 Postgres。
  - ex: ccxt 交易所实例
  - ex_name: 交易所名字字符串
  - symbol: 交易对，如 'BTC/USDT'
  - limit: 拉取条数（例如 10）
  - pg_cur, pg_conn: psycopg2 cursor & connection
  - last_ts_map: 内存记录，形如 last_ts_map[ex_name][symbol] = datetime 或 None
  行为:
    - 如果 parse 成功，把所有 timestamp > last_ts 插入
    - 使用 execute_values 做批量插入，ON CONFLICT DO NOTHING 避免重复
  """
  try:
    # 拉取 OHLCV（返回 list of lists）
    ohlcvs = ex.fetch_ohlcv(symbol, TIMEFRAME, limit=limit)
  except Exception as e:
    logging.warning("%s %s fetch_ohlcv 失败: %s", ex_name, symbol, e)
    return

  rows: List[tuple] = []  # 要批量插入的行
  for row in ohlcvs:
    parsed = parse_ohlcv_row(row)    # 解析单行
    if not parsed:
      continue
    ts_dt = parsed["timestamp"]
    # 获取当前记录的最后已写入时间戳（内存）
    last_ts = last_ts_map.get(ex_name, {}).get(symbol)
    # 只插入比 last_ts 新的行（减少重复）
    if last_ts is not None and ts_dt <= last_ts:
      continue
    rows.append((
      ex_name,
      symbol,
      TIMEFRAME,
      ts_dt,
      parsed["open"],
      parsed["high"],
      parsed["low"],
      parsed["close"],
      parsed["volume"]
    ))
    # 更新内存最后时间（保持为最新）
    if last_ts_map.get(ex_name) is not None:
      cur_last = last_ts_map[ex_name].get(symbol)
      if cur_last is None or ts_dt > cur_last:
        last_ts_map[ex_name][symbol] = ts_dt

  if not rows:
    logging.debug("%s %s: 无需插入 OHLCV（没有新行）", ex_name, symbol)
    return

  # 批量插入（ON CONFLICT DO NOTHING）
  insert_sql = """
    INSERT INTO kline_data
      (exchange, symbol, timeframe, timestamp, open, high, low, close, volume)
    VALUES %s
    ON CONFLICT DO NOTHING;
  """
  try:
    execute_values(pg_cur, insert_sql, rows)  # 高效批量
    pg_conn.commit()
    logging.info("%s %s: 批量插入 %d 条 OHLCV", ex_name, symbol, len(rows))
  except Exception as e:
    logging.exception("批量插入 %s %s 失败: %s", ex_name, symbol, e)
    try:
      pg_conn.rollback()
    except Exception:
      pass

def fetch_and_store_ticker(
  ex: ccxt.Exchange,
  ex_name: str,
  symbol: str,
  redis_client
) -> None:
  """
  拉取 ticker（实时）并写入 Redis。
  - ex: ccxt 交易所实例
  - ex_name, symbol: 元信息
  - redis_client: redis.Redis 实例
  行为:
    - 只写 Redis（轻量）以供 strategy 或监控消费
    - 如需可选地补写到 Postgres，请在调用处决定
  """
  max_retries = DEFAULT_MAX_RETRIES
  attempt = 0
  while attempt < max_retries and running:
    attempt += 1
    try:
      ticker = ex.fetch_ticker(symbol)            # 拉取 ticker（包含 last/bid/ask/volume）
      last = safe_get(ticker, "last")             # 最新成交价
      bid = safe_get(ticker, "bid")               # 买一价
      ask = safe_get(ticker, "ask")               # 卖一价
      base_vol = safe_get(ticker, "baseVolume") or safe_get(ticker, "volume")  # 交易量字段命名差异

      ts = datetime.utcnow().isoformat()          # 使用当前 UTC 时间作为写入时间（标准化）
      key = f"ticker:{ex_name}:{symbol.replace('/','').lower()}:{TIMEFRAME}"  # Redis key 约定
      payload = {
        "exchange": ex_name,
        "symbol": symbol,
        "timeframe": TIMEFRAME,
        "timestamp": ts,
        "last": last,
        "bid": bid,
        "ask": ask,
        "volume": base_vol
      }
      # 写入 Redis（字符串化 JSON，方便调试与其它语言读取）
      redis_client.set(key, json.dumps(payload))
      logging.debug("%s %s ticker 写入 Redis key=%s", ex_name, symbol, key)
      # 成功后直接返回
      return
    except Exception as e:
      wait = RETRY_BACKOFF_BASE ** attempt          # 指数退避
      logging.warning("%s %s fetch_ticker 失败（%s/%s）：%s，%.1f 秒后重试", ex_name, symbol, attempt, max_retries, type(e).__name__, wait)
      time.sleep(wait)
  logging.error("%s %s fetch_ticker 超过重试次数，放弃本轮", ex_name, symbol)

# ---------------- Main entrypoint ----------------
def main():
  """
  主程序：
    - 等待 Postgres/Redis 就绪
    - 建立连接
    - 构建交易所客户端
    - 启动主循环：ticker 高频 + 每 OHLCV_EVERY_LOOPS 批量 OHLCV 补盘
  """
  # 等待依赖就绪（防止容器编排 race）
  if not wait_for_postgres(timeout=60):
    logging.error("Postgres 无法连接，程序退出")
    return
  if not wait_for_redis(timeout=60):
    logging.error("Redis 无法连接，程序退出")
    return

  # 连接 Postgres（持久连接）
  try:
    pg_conn = psycopg2.connect(
      dbname=POSTGRES_DB,
      user=POSTGRES_USER,
      password=POSTGRES_PASSWORD,
      host=POSTGRES_HOST,
      port=POSTGRES_PORT
    )
    pg_cur = pg_conn.cursor()
  except Exception as e:
    logging.exception("连接 Postgres 失败: %s", e)
    return

  # 连接 Redis（持久连接）
  try:
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB)
    # quick ping to ensure connection
    r.ping()
  except Exception as e:
    logging.exception("连接 Redis 失败: %s", e)
    try:
      pg_cur.close()
      pg_conn.close()
    except Exception:
      pass
    return

  # 初始化交易所客户端
  exchanges = build_exchanges()
  if not exchanges:
    logging.error("没有可用的交易所客户端，退出")
    try:
      pg_cur.close()
      pg_conn.close()
    except Exception:
      pass
    return

  # 内存 map 用于记录每个 exchange/symbol 已写入的最新 timestamp（避免重复插入）
  last_insert_ts: Dict[str, Dict[str, Optional[datetime]]] = {}
  for ex_name in exchanges.keys():
    last_insert_ts[ex_name] = {}
    for sym in SYMBOLS:
      last_insert_ts[ex_name][sym] = None   # 启动时未知

  logging.info("启动 data-fetcher：exchanges=%s symbols=%s timeframe=%s interval=%s batch_ohlcv=%s every_loops=%s",
    list(exchanges.keys()), SYMBOLS, TIMEFRAME, FETCH_INTERVAL, BATCH_OHLCV_LIMIT, OHLCV_EVERY_LOOPS)

  # 启动阶段（可选）：做一次小批量补盘（确保 DB 有最近数据）
  for ex_name, ex in exchanges.items():
    for symbol in SYMBOLS:
      if not running:
        break
      try:
        # 这里我们做一次小批量的 OHLCV 补盘（limit=BATCH_OHLCV_LIMIT）
        fetch_and_store_ohlcv_batch(ex, ex_name, symbol, BATCH_OHLCV_LIMIT, pg_cur, pg_conn, last_insert_ts)
      except Exception as e:
        logging.exception("启动补盘失败 %s %s: %s", ex_name, symbol, e)

  # 主循环：每轮拉 ticker；每 OHLCV_EVERY_LOOPS 轮批量拉 OHLCV
  loop_counter = 0
  while running:
    loop_start = time.time()
    for ex_name, ex in exchanges.items():
      for symbol in SYMBOLS:
        if not running:
          break
        try:
          # 高频部分：拉 ticker 并写 Redis（低延迟 / 小开销）
          fetch_and_store_ticker(ex, ex_name, symbol, r)
        except Exception as e:
          # 单个 symbol 出错不影响其它 symbol
          logging.exception("处理 %s %s ticker 时未捕获异常：%s", ex_name, symbol, e)

        # 周期性批量 OHLCV（减少重复请求、用于补齐历史）
        try:
          if (loop_counter % OHLCV_EVERY_LOOPS) == 0:
            # 在周期触发时，拉 BATCH_OHLCV_LIMIT 条并写入 DB
            fetch_and_store_ohlcv_batch(ex, ex_name, symbol, BATCH_OHLCV_LIMIT, pg_cur, pg_conn, last_insert_ts)
        except Exception as e:
          logging.exception("周期性 OHLCV 补盘失败 %s %s: %s", ex_name, symbol, e)

    loop_counter += 1             # 增加循环计数
    # 计算循环耗时并 sleep（保证 FETCH_INTERVAL 平均间隔）
    elapsed = time.time() - loop_start
    to_sleep = max(0, FETCH_INTERVAL - elapsed)
    if to_sleep > 0:
      time.sleep(to_sleep)

  # 退出：清理连接
  try:
    pg_cur.close()
    pg_conn.close()
  except Exception:
    pass
  logging.info("data-fetcher 已优雅退出")

# ---------------- Optional: example place_order (commented) ----------------
# 下单示例（仅示例，真正实盘需全面风控与签名管理）
# def place_order_example(exchange, symbol, side, amount, price=None):
#   if price is None:
#     return exchange.create_market_order(symbol, side, amount)
#   else:
#     return exchange.create_limit_order(symbol, side, amount, price)

# ---------------- Run ----------------
if __name__ == "__main__":
  main()
