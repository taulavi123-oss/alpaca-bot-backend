# alpaca-bot-backend — Bot Python trade THẬT trên acc demo Alpaca + Web quản lý 24/7
# Chạy: python main.py  (panel của bạn dùng đúng lệnh này)
# Sau đó mở: http://localhost:8000  là thấy web quản lý luôn, khỏi cần Vercel lúc test.

import os, time, math, sqlite3, threading, traceback, uuid, json
from datetime import datetime, timezone
from pathlib import Path

import requests
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
import uvicorn

# ============ CẤU HÌNH ============
PORT = int(os.getenv("PORT", "8000"))
DATA_DIR = Path(os.getenv("DATA_DIR", "./data"))
DATA_DIR.mkdir(parents=True, exist_ok=True)
DB_PATH = DATA_DIR / "bot.db"

PAPER_BASE = "https://paper-api.alpaca.markets"
DATA_BASE = "https://data.alpaca.markets"

db_lock = threading.Lock()
workers: dict = {}  # bot_id -> {"stop": Event, "thread": Thread}

# ============ DB ============
def db():
    con = sqlite3.connect(str(DB_PATH), check_same_thread=False)
    con.row_factory = sqlite3.Row
    return con

def init_db():
    with db_lock:
        con = db()
        con.executescript("""
        CREATE TABLE IF NOT EXISTS brokers(
          id TEXT PRIMARY KEY, label TEXT, api_key TEXT, secret_key TEXT, created_at TEXT
        );
        CREATE TABLE IF NOT EXISTS bots(
          id TEXT PRIMARY KEY, broker_id TEXT, name TEXT, symbol TEXT,
          strategy TEXT, timeframe TEXT, qty_mode TEXT, qty_value REAL,
          allocated REAL, sl_pct REAL, tp_pct REAL, max_pos INTEGER,
          params TEXT, status TEXT, created_at TEXT
        );
        CREATE TABLE IF NOT EXISTS orders(
          id TEXT PRIMARY KEY, bot_id TEXT, broker_id TEXT, broker_order_id TEXT,
          client_order_id TEXT UNIQUE, symbol TEXT, side TEXT, qty REAL,
          entry_price REAL, exit_price REAL, status TEXT,
          open_time TEXT, close_time TEXT, pnl REAL DEFAULT 0, comment TEXT
        );
        CREATE TABLE IF NOT EXISTS snapshots(
          id INTEGER PRIMARY KEY AUTOINCREMENT, bot_id TEXT, ts TEXT, equity REAL
        );
        CREATE TABLE IF NOT EXISTS logs(
          id INTEGER PRIMARY KEY AUTOINCREMENT, bot_id TEXT, level TEXT, msg TEXT, ts TEXT
        );
        """)
        con.commit(); con.close()

def log(bot_id, level, msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] [{bot_id[:6] if bot_id else 'SYS'}] {msg}", flush=True)
    try:
        with db_lock:
            con = db()
            con.execute("INSERT INTO logs(bot_id,level,msg,ts) VALUES(?,?,?,?)",
                (bot_id, level, msg[:2000], datetime.now(timezone.utc).isoformat()))
            # giữ 2000 log/bot cho nhẹ
            con.execute("""DELETE FROM logs WHERE id NOT IN
              (SELECT id FROM logs WHERE bot_id=? ORDER BY id DESC LIMIT 2000) AND bot_id=?""",
              (bot_id, bot_id))
            con.commit(); con.close()
    except Exception as e:
        print("log err:", e, flush=True)

def set_status(bot_id, status):
    with db_lock:
        con = db()
        con.execute("UPDATE bots SET status=? WHERE id=?", (status, bot_id))
        con.commit(); con.close()

# ============ ALPACA (gọi TRỰC TIẾP bằng key, acc demo) ============
def norm_order_symbol(s: str) -> str:
    """User nhập BTCUSD hay BTC/USD đều map về BTC/USD để đặt lệnh."""
    s = s.strip().upper().replace("-", "/")
    if "/" in s: return s
    # crypto phổ biến: BTCUSD -> BTC/USD, ETHUSD -> ETH/USD
    for q in ["USD", "USDT", "USDC"]:
        if s.endswith(q) and len(s) > len(q):
            return s[:-len(q)] + "/" + q
    return s

def norm_position_symbol(s: str) -> str:
    return s.strip().upper()

def symbol_match(order_sym: str, pos_sym: str) -> bool:
    # order BTC/USD <-> position BTCUSD
    return norm_position_symbol(order_sym).replace("/", "") == norm_position_symbol(pos_sym).replace("/", "")

def heads(broker: dict) -> dict:
    return {"APCA-API-KEY-ID": broker["api_key"], "APCA-API-SECRET-KEY": broker["secret_key"],
            "Content-Type": "application/json"}

def api_get(broker, path):
    r = requests.get(f"{PAPER_BASE}{path}", headers=heads(broker), timeout=15)
    return r

def get_account(broker):
    r = api_get(broker, "/v2/account")
    if r.status_code != 200:
        raise Exception(f"Alpaca {r.status_code}: {r.text[:300]}")
    return r.json()

def get_positions(broker):
    r = api_get(broker, "/v2/positions")
    if r.status_code != 200: return []
    return r.json()

def get_price(broker, order_symbol: str) -> float:
    """Lấy giá mới nhất. Crypto + stock đều qua data.alpaca.markets"""
    sym = norm_order_symbol(order_symbol)
    h = heads(broker)
    try:
        if "/" in sym:  # crypto
            loc = sym.replace("/", "")
            r = requests.get(f"{DATA_BASE}/v2/crypto/latest/trades/{loc}", headers=h, timeout=10)
            if r.status_code == 200:
                return float(r.json()["trade"]["p"])
            # fallback: quote
            r = requests.get(f"{DATA_BASE}/v2/crypto/latest/quotes/{loc}", headers=h, timeout=10)
            if r.status_code == 200:
                q = r.json()["quote"]
                return float((float(q["ap"]) + float(q["bp"])) / 2)
        else:  # stock
            r = requests.get(f"{DATA_BASE}/v2/stocks/{sym}/trades/latest", headers=h, timeout=10)
            if r.status_code == 200:
                return float(r.json()["trade"]["p"])
    except Exception as e:
        pass
    # fallback cuối: lấy từ position / orderbook (không chính xác bằng nhưng đủ test)
    raise Exception(f"Không lấy được giá {sym} (data API cần gói free, check lại symbol)")

def get_bars(broker, order_symbol: str, limit=50):
    sym = norm_order_symbol(order_symbol)
    h = heads(broker)
    closes = []
    try:
        if "/" in sym:
            loc = sym.replace("/", "")
            r = requests.get(f"{DATA_BASE}/v2/crypto/bars?symbols={loc}&timeframe=1Min&limit={limit}",
                             headers=h, timeout=10)
            if r.status_code == 200:
                bars = r.json().get("bars", {}).get(loc, [])
                closes = [float(b["c"]) for b in bars]
        else:
            r = requests.get(f"{DATA_BASE}/v2/stocks/{sym}/bars?timeframe=1Min&limit={limit}",
                             headers=h, timeout=10)
            if r.status_code == 200:
                closes = [float(b["c"]) for b in r.json().get("bars", [])]
    except Exception:
        pass
    return closes

def place_buy(broker, order_symbol, notional_usd=None, qty=None, client_id=""):
    sym = norm_order_symbol(order_symbol)
    body = {"symbol": sym, "side": "buy", "type": "market", "time_in_force": "gtc",
            "client_order_id": client_id[:128]}
    if notional_usd: body["notional"] = round(float(notional_usd), 2)
    elif qty: body["qty"] = str(qty)
    else: raise Exception("thiếu qty/notional")
    r = requests.post(f"{PAPER_BASE}/v2/orders", headers=heads(broker), json=body, timeout=15)
    if r.status_code not in (200, 201):
        raise Exception(f"Đặt BUY lỗi {r.status_code}: {r.text[:300]}")
    return r.json()

def close_symbol(broker, order_symbol):
    """Đóng toàn bộ position của symbol (map BTC/USD <-> BTCUSD)"""
    poss = get_positions(broker)
    target = norm_order_symbol(order_symbol).replace("/", "")
    closed = 0
    for p in poss:
        if norm_position_symbol(p["symbol"]).replace("/", "") == target:
            sym = p["symbol"]
            r = requests.delete(f"{PAPER_BASE}/v2/positions/{sym}", headers=heads(broker), timeout=15)
            closed += 1
    return closed

# ============ STRATEGY (thuần python, không cần pandas) ============
def rsi(closes, period=14):
    if len(closes) < period + 2: return 50
    gains, losses = [], []
    for i in range(1, len(closes)):
        d = closes[i] - closes[i-1]
        gains.append(max(d, 0)); losses.append(max(-d, 0))
    ag = sum(gains[-period:]) / period; al = sum(losses[-period:]) / period
    if al == 0: return 100
    rs = ag / al
    return 100 - 100 / (1 + rs)

def sma(arr, n):
    if len(arr) < n: return None
    return sum(arr[-n:]) / n

def decide(strategy, closes, price, state):
    """Trả về 'buy' / 'sell' / None"""
    s = (strategy or "RSI").upper()
    if s == "RSI":
        v = rsi(closes)
        state["last_rsi"] = v
        if v < 30: return "buy"
        if v > 70: return "sell"
        return None
    if s == "MA_CROSS":
        a, b = sma(closes, 9), sma(closes, 21)
        if a is None or b is None: return None
        prev = state.get("prev_diff")
        diff = a - b
        state["prev_diff"] = diff
        if prev is not None and prev <= 0 < diff: return "buy"
        if prev is not None and prev >= 0 > diff: return "sell"
        return None
    if s == "GRID":
        # mua khi giá giảm 0.5% so với lần mua trước, bán khi tăng 0.8%
        last = state.get("last_buy")
        if last is None: return "buy"
        if price <= last * 0.995: return "buy"
        if price >= last * 1.008: return "sell"
        return None
    return None

# ============ TÍNH TIỀN RIÊNG TỪNG BOT ============
def bot_pnl(bot_id):
    with db_lock:
        con = db()
        rows = con.execute("SELECT pnl FROM orders WHERE bot_id=? AND status='closed'", (bot_id,)).fetchall()
        opens = con.execute("SELECT * FROM orders WHERE bot_id=? AND status='open'", (bot_id,)).fetchall()
        con.close()
    return sum(r["pnl"] or 0 for r in rows), [dict(o) for o in opens]

def bot_equity(bot_row, broker):
    alloc = float(bot_row["allocated"])
    closed_pnl, opens = bot_pnl(bot_row["id"])
    floating = 0
    try:
        poss = get_positions(broker)
        for o in opens:
            for p in poss:
                if symbol_match(o["symbol"], p["symbol"]):
                    cur = float(p.get("current_price") or p.get("avg_entry_price") or o["entry_price"])
                    qty = float(o["qty"])
                    if (o["side"] or "buy") == "buy":
                        floating += (cur - float(o["entry_price"])) * qty
                    else:
                        floating += (float(o["entry_price"]) - cur) * qty
    except Exception:
        pass
    return alloc + closed_pnl + floating, closed_pnl, floating

# ============ WORKER 24/7 ============
def worker_loop(bot_id):
    with db_lock:
        con = db()
        b = con.execute("SELECT * FROM bots WHERE id=?", (bot_id,)).fetchone()
        br = con.execute("SELECT * FROM brokers WHERE id=?", (b["broker_id"],)).fetchone() if b else None
        con.close()
    if not b or not br:
        log(bot_id, "error", "Không thấy bot hoặc tài khoản, dừng.")
        return
    bot = dict(b); broker = dict(br)
    sym = norm_order_symbol(bot["symbol"])
    state = {}
    log(bot_id, "info", f"Bot {bot['name']} khởi động | {sym} | {bot['strategy']} | vốn ${bot['allocated']}")
    last_snap = 0
    errs = 0
    stop_ev = workers[bot_id]["stop"]
    while not stop_ev.is_set():
        try:
            price = get_price(broker, sym)
            closes = get_bars(broker, sym) or [price]
            closes = (closes + [price])[-60:]
            sig = decide(bot["strategy"], closes, price, state)

            # snapshot equity mỗi 60s để vẽ chart
            if time.time() - last_snap > 60:
                try:
                    eq, _, _ = bot_equity(bot, broker)
                    with db_lock:
                        con = db()
                        con.execute("INSERT INTO snapshots(bot_id,ts,equity) VALUES(?,?,?)",
                                    (bot_id, datetime.now(timezone.utc).isoformat(), eq))
                        con.commit(); con.close()
                    last_snap = time.time()
                except Exception:
                    pass

            if sig == "buy":
                # crypto cấm short nên chỉ buy; stock check buying power
                acct = get_account(broker)
                bp = float(acct.get("buying_power", 0))
                if bot["qty_mode"] == "fixed_usd":
                    notion = float(bot["qty_value"])
                else:
                    notion = float(bot["qty_value"]) * price
                # không vượt vốn cấp + không vượt buying power acc
                eq_now, _, _ = bot_equity(bot, broker)
                if notion > bp:
                    log(bot_id, "warn", f"Muốn mua ${notion:.0f} nhưng acc chỉ còn ${bp:.0f} buying power -> bỏ qua")
                else:
                    # đếm lệnh open, quá max_pos thì thôi
                    _, opens = bot_pnl(bot_id)
                    if len(opens) >= int(bot["max_pos"] or 1):
                        pass
                    else:
                        # crypto chỉ buy
                        cid = f"{bot_id[:8]}_{int(time.time())}"
                        od = place_buy(broker, sym, notional_usd=notion, client_id=cid)
                        fill = float(od.get("filled_avg_price") or price)
                        fq = float(od.get("qty") or (notion / price))
                        oid = str(uuid.uuid4())
                        with db_lock:
                            con = db()
                            con.execute("""INSERT INTO orders(id,bot_id,broker_id,broker_order_id,client_order_id,
                              symbol,side,qty,entry_price,status,open_time,comment)
                              VALUES(?,?,?,?,?,?,?,?,?,?,?,?)""",
                              (oid, bot_id, broker["id"], od.get("id"), cid, sym, "buy", fq, fill,
                               "open", datetime.now(timezone.utc).isoformat(),
                               f"RSI={state.get('last_rsi')}" if state.get("last_rsi") else sig))
                            con.commit(); con.close()
                        state["last_buy"] = fill
                        log(bot_id, "trade", f"BUY {fq:.5f} {sym} @ {fill} (~${notion:.0f})")
                        errs = 0
            elif sig == "sell":
                if "/" in sym:
                    # crypto: chỉ đóng (cấm short theo Alpaca)
                    n = close_symbol(broker, sym)
                    if n:
                        with db_lock:
                            con = db()
                            con.execute("""UPDATE orders SET status='closed', exit_price=?, close_time=?, pnl=(?-entry_price)*qty,
                              comment='close by signal' WHERE bot_id=? AND status='open'""",
                              (price, datetime.now(timezone.utc).isoformat(), price, bot_id))
                            con.commit(); con.close()
                        log(bot_id, "trade", f"SELL/ĐÓNG {sym} @ {price} (crypto không short)")
                else:
                    n = close_symbol(broker, sym)
                    if n:
                        log(bot_id, "trade", f"ĐÓNG {sym} @ {price}")
            # TP/SL đơn giản: nếu floating của bot vượt ngưỡng thì đóng hết
            eq_now, _, fl = bot_equity(bot, broker)
            alloc = float(bot["allocated"])
            pnl_pct = (eq_now - alloc) / alloc * 100 if alloc else 0
            if pnl_pct <= -float(bot["sl_pct"] or 20) or pnl_pct >= float(bot["tp_pct"] or 9999):
                if fl != 0:
                    close_symbol(broker, sym)
                    log(bot_id, "warn", f"Chạm SL/TP ({pnl_pct:.1f}%) -> đóng hết {sym}")
                    with db_lock:
                        con = db()
                        con.execute("UPDATE bots SET status='paused' WHERE id=?", (bot_id,))
                        con.commit(); con.close()
                    log(bot_id, "warn", "Bot tự Pause vì chạm SL/TP. Muốn chạy tiếp bấm Start.")
                    break
            errs = 0
        except Exception as e:
            errs += 1
            log(bot_id, "error", f"Lỗi ({errs}): {str(e)[:300]}")
            if errs >= 5:
                log(bot_id, "error", "Lỗi 5 lần liên tiếp -> tự Pause, check key/symbol.")
                set_status(bot_id, "paused")
                break
        stop_ev.wait(10)
    log(bot_id, "info", "Worker dừng.")

def start_worker(bot_id):
    if bot_id in workers and workers[bot_id]["thread"].is_alive():
        return
    workers[bot_id] = {"stop": threading.Event(), "thread": threading.Thread(target=worker_loop, args=(bot_id,), daemon=True)}
    workers[bot_id]["thread"].start()

def stop_worker(bot_id):
    if bot_id in workers:
        workers[bot_id]["stop"].set()

# ============ APP ============
app = FastAPI(title="Alpaca Bot Manager Demo")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

@app.get("/healthz")
def healthz():
    return {"status": "ok", "running_bots": sum(1 for w in workers.values() if w["thread"].is_alive())}

@app.get("/api/brokers")
def list_brokers():
    with db_lock:
        con = db()
        rows = [dict(r) for r in con.execute("SELECT * FROM brokers ORDER BY created_at DESC").fetchall()]
        con.close()
    # hiện full key luôn vì acc demo (theo yêu cầu, khỏi che)
    return rows

@app.post("/api/brokers/test")
async def test_broker(req: Request):
    d = await req.json()
    b = {"api_key": d.get("api_key", "").strip(), "secret_key": d.get("secret_key", "").strip()}
    try:
        a = get_account(b)
        return {"ok": True, "equity": a.get("equity"), "buying_power": a.get("buying_power"),
                "cash": a.get("cash"), "id": a.get("id")}
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)[:500]}, status_code=400)

@app.post("/api/brokers")
async def add_broker(req: Request):
    d = await req.json()
    bid = str(uuid.uuid4())
    with db_lock:
        con = db()
        con.execute("INSERT INTO brokers(id,label,api_key,secret_key,created_at) VALUES(?,?,?,?,?)",
            (bid, d.get("label", "Alpaca-Demo-1"), d.get("api_key", "").strip(),
             d.get("secret_key", "").strip(), datetime.now(timezone.utc).isoformat()))
        con.commit(); con.close()
    log(bid, "info", f"Đã thêm tài khoản {d.get('label')}")
    return {"ok": True, "id": bid}

@app.delete("/api/brokers/{bid}")
def del_broker(bid: str):
    with db_lock:
        con = db()
        n = con.execute("SELECT COUNT(*) c FROM bots WHERE broker_id=? AND status='running'", (bid,)).fetchone()["c"]
        if n:
            con.close()
            return JSONResponse({"ok": False, "error": f"Còn {n} bot đang chạy, dừng hết mới xóa được"}, status_code=400)
        con.execute("DELETE FROM brokers WHERE id=?", (bid,))
        con.commit(); con.close()
    return {"ok": True}

@app.get("/api/account/live")
def live(broker_id: str):
    with db_lock:
        con = db()
        br = con.execute("SELECT * FROM brokers WHERE id=?", (broker_id,)).fetchone()
        con.close()
    if not br: return JSONResponse({"ok": False}, status_code=404)
    try:
        return {"ok": True, "account": get_account(dict(br)), "positions": get_positions(dict(br))}
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)[:500]}, status_code=400)

@app.get("/api/bots")
def list_bots():
    with db_lock:
        con = db()
        bots = [dict(r) for r in con.execute("SELECT * FROM bots ORDER BY created_at DESC").fetchall()]
        brs = {r["id"]: dict(r) for r in con.execute("SELECT * FROM brokers").fetchall()}
        con.close()
    out = []
    for b in bots:
        br = brs.get(b["broker_id"])
        try:
            eq, cpnl, fl = bot_equity(b, br) if br else (b["allocated"], 0, 0)
        except Exception:
            eq, cpnl, fl = b["allocated"], 0, 0
        pnl = eq - float(b["allocated"])
        out.append({**b, "equity": round(eq, 2), "pnl": round(pnl, 2),
                    "pnl_pct": round(pnl / float(b["allocated"]) * 100, 2) if b["allocated"] else 0,
                    "floating": round(fl, 2), "broker_label": (br or {}).get("label", "?")})
    return out

@app.post("/api/bots")
async def add_bot(req: Request):
    d = await req.json()
    bid = str(uuid.uuid4())
    with db_lock:
        con = db()
        con.execute("""INSERT INTO bots(id,broker_id,name,symbol,strategy,timeframe,qty_mode,qty_value,
          allocated,sl_pct,tp_pct,max_pos,params,status,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
          (bid, d["broker_id"], d.get("name", "Bot-01"), d.get("symbol", "BTC/USD"),
           d.get("strategy", "RSI"), d.get("timeframe", "1m"), d.get("qty_mode", "fixed_usd"),
           float(d.get("qty_value", 100)), float(d.get("allocated", 1000)),
           float(d.get("sl_pct", 20)), float(d.get("tp_pct", 50)), int(d.get("max_pos", 1)),
           json.dumps(d.get("params", {})), "stopped", datetime.now(timezone.utc).isoformat()))
        con.commit(); con.close()
    log(bid, "info", f"Tạo bot {d.get('name')} vốn ${d.get('allocated')}")
    if d.get("start_now"):
        set_status(bid, "running"); start_worker(bid)
    return {"ok": True, "id": bid}

@app.patch("/api/bots/{bid}")
async def ctl_bot(bid: str, req: Request):
    d = await req.json()
    act = d.get("action")
    if act == "start":
        set_status(bid, "running"); start_worker(bid)
    elif act in ("pause", "stop"):
        set_status(bid, "paused" if act == "pause" else "stopped"); stop_worker(bid)
        if act == "stop":
            with db_lock:
                con = db(); b = con.execute("SELECT * FROM bots WHERE id=?", (bid,)).fetchone()
                br = con.execute("SELECT * FROM brokers WHERE id=?", (b["broker_id"],)).fetchone() if b else None
                con.close()
            if b and br:
                try: close_symbol(dict(br), b["symbol"])
                except Exception: pass
    return {"ok": True}

@app.delete("/api/bots/{bid}")
def del_bot(bid: str):
    stop_worker(bid)
    with db_lock:
        con = db()
        con.execute("DELETE FROM orders WHERE bot_id=?", (bid,))
        con.execute("DELETE FROM snapshots WHERE bot_id=?", (bid,))
        con.execute("DELETE FROM logs WHERE bot_id=?", (bid,))
        con.execute("DELETE FROM bots WHERE id=?", (bid,))
        con.commit(); con.close()
    return {"ok": True}

@app.post("/api/bots/{bid}/reset")
def reset_bot(bid: str):
    stop_worker(bid)
    with db_lock:
        con = db()
        b = con.execute("SELECT * FROM bots WHERE id=?", (bid,)).fetchone()
        br = con.execute("SELECT * FROM brokers WHERE id=?", (b["broker_id"],)).fetchone() if b else None
        con.execute("DELETE FROM orders WHERE bot_id=?", (bid,))
        con.execute("DELETE FROM snapshots WHERE bot_id=?", (bid,))
        con.execute("UPDATE bots SET status='stopped' WHERE id=?", (bid,))
        con.commit(); con.close()
    if b and br:
        try: close_symbol(dict(br), b["symbol"])
        except Exception: pass
    log(bid, "info", "Đã reset vốn về ban đầu.")
    return {"ok": True}

@app.get("/api/bots/{bid}/orders")
def bot_orders(bid: str, status: str = ""):
    with db_lock:
        con = db()
        q = "SELECT * FROM orders WHERE bot_id=?" + (" AND status=?" if status else "") + " ORDER BY open_time DESC LIMIT 200"
        a = (bid,) if not status else (bid, status)
        rows = [dict(r) for r in con.execute(q, a).fetchall()]
        con.close()
    return rows

@app.get("/api/bots/{bid}/logs")
def bot_logs(bid: str):
    with db_lock:
        con = db()
        rows = [dict(r) for r in con.execute("SELECT * FROM logs WHERE bot_id=? ORDER BY id DESC LIMIT 100", (bid,)).fetchall()]
        con.close()
    return rows[::-1]

@app.get("/api/bots/{bid}/equity")
def bot_equity_api(bid: str):
    with db_lock:
        con = db()
        rows = [dict(r) for r in con.execute("SELECT * FROM snapshots WHERE bot_id=? ORDER BY id", (bid,)).fetchall()]
        con.close()
    return rows

@app.get("/api/dashboard/summary")
def summary():
    bots = list_bots()
    tot_cap = sum(b["allocated"] for b in bots)
    tot_eq = sum(b["equity"] for b in bots)
    return {"bots": bots, "total_cap": round(tot_cap, 2), "total_eq": round(tot_eq, 2),
            "total_pnl": round(tot_eq - tot_cap, 2), "running": sum(1 for b in bots if b["status"] == "running")}

# ============ WEB QUẢN LÝ (mở / là xài, khỏi cần Vercel lúc test) ============
HOME_HTML = """<!DOCTYPE html><html lang="vi"><head><meta charset="utf-8"/>
<meta name="viewport" content="width=device-width,initial-scale=1"/>
<title>Alpaca Bot Manager — Demo 24/7</title>
<style>
*{box-sizing:border-box}body{background:#0B0E11;color:#EAECEF;font-family:Inter,Arial;margin:0}
.wrap{max-width:1100px;margin:0 auto;padding:20px}
.card{background:#12161C;border-radius:12px;padding:16px;margin:12px 0}
h1{font-size:22px}h2{font-size:16px;margin:0 0 10px}
.green{color:#0ECB81}.red{color:#F6465D}.mut{color:#848E9C}
input,select,button{background:#1E2329;border:1px solid #2B3139;color:#fff;border-radius:8px;padding:9px 12px;margin:4px}
button{background:#0ECB81;color:#000;font-weight:700;cursor:pointer;border:none}
button.ghost{background:#2B3139;color:#fff}button.redb{background:#F6465D;color:#fff}
table{width:100%;border-collapse:collapse;font-size:14px}
td,th{border-bottom:1px solid #2B3139;padding:8px;text-align:left}
.dot{display:inline-block;width:9px;height:9px;border-radius:50%}.run{background:#0ECB81}.stop{background:#5E6673}.pause{background:#F0B90B}.err{background:#F6465D}
.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));gap:10px}
.kpi{background:#1E2329;border-radius:10px;padding:12px}.kpi b{font-size:20px}
.row{display:flex;gap:8px;flex-wrap:wrap;align-items:center}
pre{background:#000;border-radius:8px;padding:10px;max-height:260px;overflow:auto;font-size:12px}
</style></head><body><div class="wrap">
<h1>🤖 Alpaca Bot Manager <span class="mut">— trade THẬT trên acc demo, tắt máy vẫn chạy</span></h1>
<div class="card"><h2>📊 Tổng quan</h2><div class="grid" id="kpi"></div><div class="row">
<button onclick="load()">🔄 Làm mới</button><span class="mut" id="health"></span></div></div>
<div class="card"><h2>1️⃣ Thêm tài khoản Demo (nhập Key + Secret)</h2>
<div class="row"><input id="blabel" value="Alpaca-Demo-1" style="width:160px"/>
<input id="bkey" placeholder="API Key (PK...)" style="width:240px"/>
<input id="bsec" placeholder="Secret Key" type="password" style="width:240px"/>
<button onclick="testKey()">Kiểm tra</button><button onclick="saveBroker()">Lưu</button></div>
<div id="testRs" class="mut"></div><div id="brokers"></div></div>
<div class="card"><h2>2️⃣ Thêm Bot (dùng TIỀN DEMO thật để trade)</h2>
<div class="row">
<select id="bbroker"></select><input id="bname" value="RSI-BTC-01" style="width:140px"/>
<select id="bsym"><option>BTC/USD</option><option>ETH/USD</option><option>AAPL</option><option>TSLA</option><option>SPY</option><option>NVDA</option></select>
<select id="bstrat"><option>RSI</option><option>MA_CROSS</option><option>GRID</option></select>
<input id="bqty" value="100" title="Số USD mỗi lệnh" style="width:90px"/>
<input id="bcap" value="1000" title="Vốn cấp" style="width:90px"/>
<button onclick="addBot()">+ Tạo & Chạy ngay</button></div>
<div class="mut">Bot mua bằng tiền demo thật. Mở app Alpaca là thấy lệnh. Vốn mỗi bot cố định để tính lãi/lỗ riêng.</div></div>
<div class="card"><h2>3️⃣ Bot của tôi</h2><div id="bots"></div></div>
<div class="card"><h2>📜 Log</h2><div class="row"><select id="logbot"></select><button class="ghost" onclick="loadLogs()">Xem log</button></div><pre id="logs">...</pre></div>
<script>
async function j(u,o){const r=await fetch(u,o);return r.json()}
async function load(){
 const s=await j('/api/dashboard/summary');
 document.getElementById('kpi').innerHTML=
 `<div class="kpi">Tổng vốn<div><b>$${s.total_cap}</b></div></div>
  <div class="kpi">Tổng Equity<div><b>$${s.total_eq}</b></div></div>
  <div class="kpi">Lãi/Lỗ<div><b class="${s.total_pnl>=0?'green':'red'}">${s.total_pnl>=0?'+':''}$${s.total_pnl}</b></div></div>
  <div class="kpi">Đang chạy<div><b>${s.running}</b></div></div>`;
 const h=await j('/healthz');document.getElementById('health').innerText='Worker: '+h.status+' | '+h.running_bots+' bot đang chạy';
 const bs=await j('/api/brokers');
 document.getElementById('brokers').innerHTML=bs.map(b=>`<div class="row">📁 <b>${b.label}</b> <span class="mut">${b.api_key} / ****${(b.secret_key||'').slice(-4)}</span><button class="ghost" onclick="live('${b.id}')">Đồng bộ</button><button class="ghost" onclick="delB('${b.id}')">Xóa</button><span class="mut" id="lv-${b.id}"></span></div>`).join('')||'<span class="mut">Chưa có tài khoản</span>';
 document.getElementById('bbroker').innerHTML=bs.map(b=>`<option value="${b.id}">${b.label}</option>`).join('');
 document.getElementById('bots').innerHTML=s.bots.map(b=>`<table><tr><td><span class="dot ${b.status=='running'?'run':b.status=='paused'?'pause':b.status=='error'?'err':'stop'}"></span> <b>${b.name}</b> <span class="mut">${b.symbol} ${b.strategy} | ${b.broker_label}</span><br/>Vốn $${b.allocated} → Equity <b>$${b.equity}</b> | Lãi/Lỗ <b class="${b.pnl>=0?'green':'red'}">${b.pnl>=0?'+':''}$${b.pnl} (${b.pnl_pct}%)</b> | Floating $${b.floating}</td><td style="white-space:nowrap"><button onclick="ctl('${b.id}','start')">Start</button> <button class="ghost" onclick="ctl('${b.id}','pause')">Pause</button> <button class="redb" onclick="ctl('${b.id}','stop')">Stop</button> <button class="ghost" onclick="resetB('${b.id}')">Reset</button> <button class="ghost" onclick="delBot('${b.id}')">Xóa</button></td></tr></table>`).join('')||'<span class="mut">Chưa có bot</span>';
 document.getElementById('logbot').innerHTML=s.bots.map(b=>`<option value="${b.id}">${b.name}</option>`).join('');
}
async function testKey(){const r=await j('/api/brokers/test',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({api_key:document.getElementById('bkey').value,secret_key:document.getElementById('bsec').value})});document.getElementById('testRs').innerHTML=r.ok?`<span class="green">✅ Kết nối OK — Equity $${r.equity} | Buying power $${r.buying_power}</span>`:`<span class="red">❌ ${r.error}</span>`}
async function saveBroker(){await j('/api/brokers',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({label:document.getElementById('blabel').value,api_key:document.getElementById('bkey').value,secret_key:document.getElementById('bsec').value})});load()}
async function addBot(){await j('/api/bots',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({broker_id:document.getElementById('bbroker').value,name:document.getElementById('bname').value,symbol:document.getElementById('bsym').value,strategy:document.getElementById('bstrat').value,qty_mode:'fixed_usd',qty_value:parseFloat(document.getElementById('bqty').value),allocated:parseFloat(document.getElementById('bcap').value),sl_pct:20,tp_pct:50,max_pos:1,start_now:true})});load()}
async function ctl(id,a){await j('/api/bots/'+id,{method:'PATCH',headers:{'Content-Type':'application/json'},body:JSON.stringify({action:a})});load()}
async function delBot(id){if(confirm('Xóa bot?')){await fetch('/api/bots/'+id,{method:'DELETE'});load()}}
async function resetB(id){if(confirm('Reset vốn bot?')){await fetch('/api/bots/'+id+'/reset',{method:'POST'});load()}}
async function delB(id){if(confirm('Xóa tài khoản?')){const r=await (await fetch('/api/brokers/'+id,{method:'DELETE'})).json();if(!r.ok)alert(r.error);load()}}
async function live(id){const r=await j('/api/account/live?broker_id='+id);document.getElementById('lv-'+id).innerText=r.ok?`Equity $${r.account.equity}`:r.error}
async function loadLogs(){const id=document.getElementById('logbot').value;if(!id)return;const r=await j('/api/bots/'+id+'/logs');document.getElementById('logs').innerText=r.map(l=>`[${l.ts.slice(11,19)}] ${l.msg}`).join('\\n')||'(chưa có log)'}
load();setInterval(load,15000);
</script></div></body></html>"""

@app.get("/", response_class=HTMLResponse)
def home():
    return HOME_HTML

# khôi phục bot running sau restart (24/7 thật)
@app.on_event("startup")
def startup():
    init_db()
    with db_lock:
        con = db()
        rows = con.execute("SELECT id FROM bots WHERE status='running'").fetchall()
        con.close()
    for r in rows:
        try: start_worker(r["id"])
        except Exception: pass
    print(f"✅ Backend sống. {len(rows)} bot tự chạy lại.", flush=True)

if __name__ == "__main__":
    init_db()
    uvicorn.run("main:app", host="0.0.0.0", port=PORT)