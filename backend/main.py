"""
TradeDesk Backend — FastAPI
============================
Endpoints:
  POST /auth/register
  POST /auth/login
  GET  /auth/me
  GET  /trades              — get all trades for logged-in user
  POST /trades              — create trade manually
  PUT  /trades/{id}         — update notes/emotion/setup on a trade
  DELETE /trades/{id}       — delete trade
  POST /trades/sync         — MT5 sync (bulk upsert from sync script)
  GET  /stats               — aggregated stats
  GET  /trades/export       — CSV download
"""

from fastapi import FastAPI, Depends, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from fastapi.responses import StreamingResponse
from sqlalchemy import create_engine, Column, String, Float, Integer, DateTime, Boolean, Text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session
from pydantic import BaseModel
from typing import Optional, List
from datetime import datetime, timedelta
import hashlib, jwt, os, csv, io, uuid

# ── CONFIG ───────────────────────────────────────────────
DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///./tradedesk.db")
SECRET_KEY   = os.getenv("SECRET_KEY", "tradedesk-secret-change-in-production")
ALGORITHM    = "HS256"
TOKEN_EXPIRE = 30  # days

# Fix postgres URL for SQLAlchemy
if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

# ── DATABASE ─────────────────────────────────────────────
engine  = create_engine(DATABASE_URL, connect_args={"check_same_thread": False} if "sqlite" in DATABASE_URL else {})
Session_ = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base    = declarative_base()


class UserDB(Base):
    __tablename__ = "users"
    id         = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    username   = Column(String, unique=True, index=True, nullable=False)
    name       = Column(String, nullable=False)
    pass_hash  = Column(String, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)
    mt5_login  = Column(String, nullable=True)    # MT5 account number
    mt5_server = Column(String, nullable=True)    # MT5 broker server


class TradeDB(Base):
    __tablename__ = "trades"
    id            = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    user_id       = Column(String, index=True, nullable=False)
    mt5_ticket    = Column(String, nullable=True)   # MT5 ticket number — for dedup
    symbol        = Column(String, nullable=False)
    direction     = Column(String, nullable=False)   # BUY / SELL
    open_time     = Column(DateTime, nullable=True)
    close_time    = Column(DateTime, nullable=True)
    open_price    = Column(Float, nullable=True)
    close_price   = Column(Float, nullable=True)
    sl            = Column(Float, nullable=True)
    tp            = Column(Float, nullable=True)
    volume        = Column(Float, nullable=True)
    profit        = Column(Float, nullable=False, default=0)
    swap          = Column(Float, nullable=True, default=0)
    commission    = Column(Float, nullable=True, default=0)
    duration_min  = Column(Integer, nullable=True)
    result        = Column(String, nullable=True)    # WIN / LOSS / BE
    setup         = Column(String, nullable=True)    # manual
    session       = Column(String, nullable=True)    # auto-detected from open_time
    notes         = Column(Text, nullable=True)      # manual
    emotion       = Column(Integer, nullable=True)   # manual 1-10
    risk          = Column(Float, nullable=True)
    r_multiple    = Column(Float, nullable=True)
    is_manual     = Column(Boolean, default=False)   # true = typed by hand
    created_at    = Column(DateTime, default=datetime.utcnow)


Base.metadata.create_all(bind=engine)

# ── APP ──────────────────────────────────────────────────
app = FastAPI(title="TradeDesk API", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],   # tighten in production
    allow_methods=["*"],
    allow_headers=["*"],
)

security = HTTPBearer()


def get_db():
    db = Session_()
    try:
        yield db
    finally:
        db.close()


# ── AUTH HELPERS ─────────────────────────────────────────
def hash_password(password: str) -> str:
    return hashlib.sha256(password.encode()).hexdigest()


def make_token(user_id: str) -> str:
    expire = datetime.utcnow() + timedelta(days=TOKEN_EXPIRE)
    return jwt.encode({"sub": user_id, "exp": expire}, SECRET_KEY, algorithm=ALGORITHM)


def get_current_user(
    credentials: HTTPAuthorizationCredentials = Depends(security),
    db: Session = Depends(get_db)
) -> UserDB:
    try:
        payload = jwt.decode(credentials.credentials, SECRET_KEY, algorithms=[ALGORITHM])
        user_id = payload.get("sub")
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Token expired")
    except Exception:
        raise HTTPException(status_code=401, detail="Invalid token")

    user = db.query(UserDB).filter(UserDB.id == user_id).first()
    if not user:
        raise HTTPException(status_code=401, detail="User not found")
    return user


# ── SESSION DETECTION ────────────────────────────────────
def detect_session(dt: Optional[datetime]) -> str:
    if not dt:
        return "UNKNOWN"
    h = dt.hour
    if 7  <= h < 12: return "LONDON"
    if 12 <= h < 17: return "NEW YORK"
    if 12 <= h < 14: return "OVERLAP"
    if 0  <= h <  7: return "ASIA"
    return "OFF-HOURS"


def detect_result(profit: float) -> str:
    if profit > 0.5:   return "WIN"
    if profit < -0.5:  return "LOSS"
    return "BE"


# ── SCHEMAS ──────────────────────────────────────────────
class RegisterSchema(BaseModel):
    username: str
    name:     str
    password: str


class LoginSchema(BaseModel):
    username: str
    password: str


class TradeCreateSchema(BaseModel):
    symbol:      str
    direction:   str
    open_time:   Optional[str]
    close_time:  Optional[str]
    open_price:  Optional[float]
    close_price: Optional[float]
    sl:          Optional[float]
    tp:          Optional[float]
    volume:      Optional[float]
    profit:      float
    setup:       Optional[str]
    notes:       Optional[str]
    emotion:     Optional[int]
    risk:        Optional[float]


class TradeUpdateSchema(BaseModel):
    notes:   Optional[str]
    emotion: Optional[int]
    setup:   Optional[str]
    result:  Optional[str]
    risk:    Optional[float]


class MT5TradeSchema(BaseModel):
    ticket:      str
    symbol:      str
    direction:   str
    open_time:   str
    close_time:  str
    open_price:  float
    close_price: float
    sl:          float
    tp:          float
    volume:      float
    profit:      float
    swap:        Optional[float] = 0
    commission:  Optional[float] = 0


class MT5SyncSchema(BaseModel):
    trades: List[MT5TradeSchema]


class MT5CredentialsSchema(BaseModel):
    mt5_login:  str
    mt5_server: str


# ── AUTH ROUTES ──────────────────────────────────────────
@app.post("/auth/register")
def register(body: RegisterSchema, db: Session = Depends(get_db)):
    username = body.username.strip().lower()

    if len(username) < 3:
        raise HTTPException(400, "Username must be at least 3 characters")
    if not username.replace("_", "").isalnum():
        raise HTTPException(400, "Username: only letters, numbers, underscore")
    if len(body.password) < 4:
        raise HTTPException(400, "Password must be at least 4 characters")
    if db.query(UserDB).filter(UserDB.username == username).first():
        raise HTTPException(400, "Username already taken")

    user = UserDB(
        username  = username,
        name      = body.name.strip(),
        pass_hash = hash_password(body.password)
    )
    db.add(user)
    db.commit()
    db.refresh(user)

    return {"token": make_token(user.id), "user": _user_dict(user)}


@app.post("/auth/login")
def login(body: LoginSchema, db: Session = Depends(get_db)):
    username = body.username.strip().lower()
    user = db.query(UserDB).filter(UserDB.username == username).first()

    if not user or user.pass_hash != hash_password(body.password):
        raise HTTPException(401, "Invalid username or password")

    return {"token": make_token(user.id), "user": _user_dict(user)}


@app.get("/auth/me")
def me(user: UserDB = Depends(get_current_user)):
    return _user_dict(user)


@app.put("/auth/mt5-credentials")
def save_mt5_credentials(
    body: MT5CredentialsSchema,
    user: UserDB = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    user.mt5_login  = body.mt5_login
    user.mt5_server = body.mt5_server
    db.commit()
    return {"message": "MT5 credentials saved"}


def _user_dict(user: UserDB):
    return {
        "id":         user.id,
        "username":   user.username,
        "name":       user.name,
        "created_at": str(user.created_at),
        "mt5_login":  user.mt5_login,
        "mt5_server": user.mt5_server,
    }


# ── TRADE ROUTES ─────────────────────────────────────────
@app.get("/trades")
def get_trades(
    user: UserDB = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    trades = db.query(TradeDB)\
               .filter(TradeDB.user_id == user.id)\
               .order_by(TradeDB.open_time.desc())\
               .all()
    return [_trade_dict(t) for t in trades]


@app.post("/trades")
def create_trade(
    body: TradeCreateSchema,
    user: UserDB = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    open_dt  = _parse_dt(body.open_time)
    close_dt = _parse_dt(body.close_time)
    duration = None
    if open_dt and close_dt:
        duration = int((close_dt - open_dt).total_seconds() / 60)

    trade = TradeDB(
        user_id      = user.id,
        symbol       = body.symbol,
        direction    = body.direction,
        open_time    = open_dt,
        close_time   = close_dt,
        open_price   = body.open_price,
        close_price  = body.close_price,
        sl           = body.sl,
        tp           = body.tp,
        volume       = body.volume,
        profit       = body.profit,
        duration_min = duration,
        result       = detect_result(body.profit),
        session      = detect_session(open_dt),
        setup        = body.setup,
        notes        = body.notes,
        emotion      = body.emotion,
        risk         = body.risk,
        r_multiple   = round(body.profit / body.risk, 2) if body.risk and body.risk != 0 else None,
        is_manual    = True
    )
    db.add(trade)
    db.commit()
    db.refresh(trade)
    return _trade_dict(trade)


@app.put("/trades/{trade_id}")
def update_trade(
    trade_id: str,
    body: TradeUpdateSchema,
    user: UserDB = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    trade = db.query(TradeDB).filter(
        TradeDB.id == trade_id,
        TradeDB.user_id == user.id
    ).first()
    if not trade:
        raise HTTPException(404, "Trade not found")

    if body.notes   is not None: trade.notes   = body.notes
    if body.emotion is not None: trade.emotion = body.emotion
    if body.setup   is not None: trade.setup   = body.setup
    if body.result  is not None: trade.result  = body.result
    if body.risk    is not None:
        trade.risk       = body.risk
        trade.r_multiple = round(trade.profit / body.risk, 2) if body.risk != 0 else None

    db.commit()
    db.refresh(trade)
    return _trade_dict(trade)


@app.delete("/trades/{trade_id}")
def delete_trade(
    trade_id: str,
    user: UserDB = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    trade = db.query(TradeDB).filter(
        TradeDB.id == trade_id,
        TradeDB.user_id == user.id
    ).first()
    if not trade:
        raise HTTPException(404, "Trade not found")
    db.delete(trade)
    db.commit()
    return {"message": "Deleted"}


# ── MT5 SYNC ROUTE ───────────────────────────────────────
@app.post("/trades/sync")
def sync_mt5_trades(
    body: MT5SyncSchema,
    user: UserDB = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """
    Called by the MT5 sync script running on user's PC.
    Upserts trades by ticket number — no duplicates.
    """
    added   = 0
    updated = 0

    for t in body.trades:
        open_dt  = _parse_dt(t.open_time)
        close_dt = _parse_dt(t.close_time)
        duration = None
        if open_dt and close_dt:
            duration = int((close_dt - open_dt).total_seconds() / 60)

        # Check if trade already exists by ticket
        existing = db.query(TradeDB).filter(
            TradeDB.user_id    == user.id,
            TradeDB.mt5_ticket == t.ticket
        ).first()

        if existing:
            # Update financial data but preserve manual fields
            existing.profit      = t.profit
            existing.close_price = t.close_price
            existing.close_time  = close_dt
            existing.sl          = t.sl
            existing.tp          = t.tp
            existing.swap        = t.swap
            existing.commission  = t.commission
            existing.duration_min = duration
            existing.result      = detect_result(t.profit)
            if existing.risk:
                existing.r_multiple = round(t.profit / existing.risk, 2)
            db.commit()
            updated += 1
        else:
            # New trade — auto-fill everything
            new_trade = TradeDB(
                user_id      = user.id,
                mt5_ticket   = t.ticket,
                symbol       = t.symbol,
                direction    = t.direction,
                open_time    = open_dt,
                close_time   = close_dt,
                open_price   = t.open_price,
                close_price  = t.close_price,
                sl           = t.sl,
                tp           = t.tp,
                volume       = t.volume,
                profit       = t.profit,
                swap         = t.swap,
                commission   = t.commission,
                duration_min = duration,
                result       = detect_result(t.profit),
                session      = detect_session(open_dt),
                is_manual    = False
            )
            db.add(new_trade)
            db.commit()
            added += 1

    return {
        "message": "Sync complete",
        "added":   added,
        "updated": updated,
        "total":   len(body.trades)
    }


# ── STATS ROUTE ──────────────────────────────────────────
@app.get("/stats")
def get_stats(
    user: UserDB = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    trades = db.query(TradeDB).filter(TradeDB.user_id == user.id).all()
    if not trades:
        return {"total": 0}

    wins   = [t for t in trades if t.result == "WIN"]
    losses = [t for t in trades if t.result == "LOSS"]
    pnl    = sum(t.profit for t in trades)
    gp     = sum(t.profit for t in wins)
    gl     = abs(sum(t.profit for t in losses))
    pf     = gp / gl if gl > 0 else (999 if gp > 0 else 0)

    # Drawdown
    eq = 0; peak = 0; dd = 0
    for t in sorted(trades, key=lambda x: x.open_time or datetime.utcnow()):
        eq += t.profit
        if eq > peak: peak = eq
        if peak - eq > dd: dd = peak - eq

    # By setup
    by_setup = {}
    for t in trades:
        k = t.setup or "Unknown"
        if k not in by_setup: by_setup[k] = {"wins": 0, "total": 0, "pnl": 0}
        by_setup[k]["total"] += 1
        by_setup[k]["pnl"]   += t.profit
        if t.result == "WIN": by_setup[k]["wins"] += 1

    # By session
    by_session = {}
    for t in trades:
        k = t.session or "Unknown"
        if k not in by_session: by_session[k] = {"wins": 0, "total": 0, "pnl": 0}
        by_session[k]["total"] += 1
        by_session[k]["pnl"]   += t.profit
        if t.result == "WIN": by_session[k]["wins"] += 1

    # By symbol
    by_symbol = {}
    for t in trades:
        k = t.symbol
        if k not in by_symbol: by_symbol[k] = {"wins": 0, "total": 0, "pnl": 0}
        by_symbol[k]["total"] += 1
        by_symbol[k]["pnl"]   += t.profit
        if t.result == "WIN": by_symbol[k]["wins"] += 1

    # Day of week
    dow = [0.0] * 7
    for t in trades:
        if t.open_time:
            dow[t.open_time.weekday()] += t.profit

    avg_hold = sum((t.duration_min or 0) for t in trades) / len(trades)
    best  = max(t.profit for t in trades)
    worst = min(t.profit for t in trades)

    return {
        "total":      len(trades),
        "wins":       len(wins),
        "losses":     len(losses),
        "winrate":    round(len(wins) / len(trades) * 100, 1),
        "pnl":        round(pnl, 2),
        "profit_factor": round(pf, 2),
        "expectancy": round(pnl / len(trades), 2),
        "max_drawdown": round(dd, 2),
        "best_trade": round(best, 2),
        "worst_trade": round(worst, 2),
        "avg_hold_min": round(avg_hold, 0),
        "by_setup":   by_setup,
        "by_session": by_session,
        "by_symbol":  by_symbol,
        "dow_pnl":    [round(x, 2) for x in dow],
    }


# ── EXPORT ROUTE ─────────────────────────────────────────
@app.get("/trades/export")
def export_trades(
    user: UserDB = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    trades = db.query(TradeDB)\
               .filter(TradeDB.user_id == user.id)\
               .order_by(TradeDB.open_time.desc())\
               .all()

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow([
        "Ticket", "Symbol", "Direction", "Open Time", "Close Time",
        "Open Price", "Close Price", "SL", "TP", "Volume",
        "Profit", "Swap", "Commission", "Duration (min)",
        "Result", "Session", "Setup", "R Multiple", "Emotion", "Notes"
    ])
    for t in trades:
        writer.writerow([
            t.mt5_ticket, t.symbol, t.direction,
            t.open_time, t.close_time,
            t.open_price, t.close_price, t.sl, t.tp, t.volume,
            t.profit, t.swap, t.commission, t.duration_min,
            t.result, t.session, t.setup, t.r_multiple,
            t.emotion, t.notes
        ])

    output.seek(0)
    filename = f"tradedesk_{user.username}_{datetime.utcnow().strftime('%Y%m%d')}.csv"
    return StreamingResponse(
        iter([output.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename={filename}"}
    )


# ── HELPERS ──────────────────────────────────────────────
def _parse_dt(s: Optional[str]) -> Optional[datetime]:
    if not s:
        return None
    formats = [
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M:%S.%f",
        "%Y.%m.%d %H:%M:%S",
        "%Y-%m-%d",
    ]
    for fmt in formats:
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            continue
    return None


def _trade_dict(t: TradeDB) -> dict:
    return {
        "id":           t.id,
        "mt5_ticket":   t.mt5_ticket,
        "symbol":       t.symbol,
        "direction":    t.direction,
        "open_time":    str(t.open_time)  if t.open_time  else None,
        "close_time":   str(t.close_time) if t.close_time else None,
        "open_price":   t.open_price,
        "close_price":  t.close_price,
        "sl":           t.sl,
        "tp":           t.tp,
        "volume":       t.volume,
        "profit":       t.profit,
        "swap":         t.swap,
        "commission":   t.commission,
        "duration_min": t.duration_min,
        "result":       t.result,
        "session":      t.session,
        "setup":        t.setup,
        "notes":        t.notes,
        "emotion":      t.emotion,
        "risk":         t.risk,
        "r_multiple":   t.r_multiple,
        "is_manual":    t.is_manual,
        "created_at":   str(t.created_at),
    }


@app.get("/")
def root():
    return {"status": "TradeDesk API running", "version": "1.0.0"}
