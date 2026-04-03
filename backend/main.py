"""
TradeDesk Backend — FastAPI
============================
Fixed version:
- PostgreSQL persistent database (no more data loss on Railway restart)
- Proper JWT token handling
- CORS fixed for all origins
"""

from fastapi import FastAPI, Depends, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from fastapi.responses import StreamingResponse
from sqlalchemy import create_engine, Column, String, Float, Integer, DateTime, Boolean, Text, text
try:
    from sqlalchemy.orm import declarative_base
except ImportError:
    from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session
from pydantic import BaseModel
from typing import Optional, List
from datetime import datetime, timedelta
import hashlib, os, csv, io, uuid

# PyJWT import with fallback
try:
    import jwt
    def encode_jwt(payload):
        return jwt.encode(payload, SECRET_KEY, algorithm=ALGORITHM)
    def decode_jwt(token):
        return jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
except Exception:
    import jose.jwt as jwt
    def encode_jwt(payload):
        return jwt.encode(payload, SECRET_KEY, algorithm=ALGORITHM)
    def decode_jwt(token):
        return jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])

# ── CONFIG ────────────────────────────────────────────────
DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///./tradedesk.db")
SECRET_KEY   = os.getenv("SECRET_KEY",   "tradedesk-dev-secret-key-change-me")
ALGORITHM    = "HS256"
TOKEN_DAYS   = 30

# Railway/Heroku use postgres:// but SQLAlchemy needs postgresql://
if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

# ── DATABASE ──────────────────────────────────────────────
connect_args = {"check_same_thread": False} if "sqlite" in DATABASE_URL else {}
engine   = create_engine(DATABASE_URL, connect_args=connect_args, pool_pre_ping=True)
SessionL = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base     = declarative_base()


class UserDB(Base):
    __tablename__ = "users"
    id         = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    username   = Column(String, unique=True, index=True, nullable=False)
    name       = Column(String, nullable=False)
    pass_hash  = Column(String, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)
    mt5_login  = Column(String, nullable=True)
    mt5_server = Column(String, nullable=True)


class TradeDB(Base):
    __tablename__ = "trades"
    id           = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    user_id      = Column(String, index=True, nullable=False)
    mt5_ticket   = Column(String, nullable=True)
    symbol       = Column(String, nullable=False)
    direction    = Column(String, nullable=False)
    open_time    = Column(DateTime, nullable=True)
    close_time   = Column(DateTime, nullable=True)
    open_price   = Column(Float,    nullable=True)
    close_price  = Column(Float,    nullable=True)
    sl           = Column(Float,    nullable=True)
    tp           = Column(Float,    nullable=True)
    volume       = Column(Float,    nullable=True)
    profit       = Column(Float,    nullable=False, default=0)
    swap         = Column(Float,    nullable=True,  default=0)
    commission   = Column(Float,    nullable=True,  default=0)
    duration_min = Column(Integer,  nullable=True)
    result       = Column(String,   nullable=True)
    setup        = Column(String,   nullable=True)
    session      = Column(String,   nullable=True)
    notes        = Column(Text,     nullable=True)
    emotion      = Column(Integer,  nullable=True)
    risk         = Column(Float,    nullable=True)
    r_multiple   = Column(Float,    nullable=True)
    is_manual    = Column(Boolean,  default=False)
    created_at   = Column(DateTime, default=datetime.utcnow)


# Create tables
Base.metadata.create_all(bind=engine)

# ── APP ───────────────────────────────────────────────────
app = FastAPI(title="TradeDesk API", version="2.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

security = HTTPBearer(auto_error=False)


def get_db():
    db = SessionL()
    try:
        yield db
    finally:
        db.close()


# ── AUTH HELPERS ──────────────────────────────────────────
def hash_pw(password: str) -> str:
    return hashlib.sha256(password.encode("utf-8")).hexdigest()


def make_token(user_id: str) -> str:
    expire  = datetime.utcnow() + timedelta(days=TOKEN_DAYS)
    payload = {"sub": user_id, "exp": expire}
    token   = encode_jwt(payload)
    # PyJWT can return bytes in older versions
    if isinstance(token, bytes):
        token = token.decode("utf-8")
    return token


def verify_token(
    credentials: HTTPAuthorizationCredentials = Depends(security),
    db: Session = Depends(get_db)
) -> UserDB:
    if not credentials:
        raise HTTPException(status_code=401, detail="Not authenticated")
    try:
        payload = decode_jwt(credentials.credentials)
        user_id = payload.get("sub")
        if not user_id:
            raise HTTPException(status_code=401, detail="Invalid token")
    except Exception as e:
        raise HTTPException(status_code=401, detail="Token invalid or expired: " + str(e))

    user = db.query(UserDB).filter(UserDB.id == user_id).first()
    if not user:
        raise HTTPException(status_code=401, detail="User not found — please log in again")
    return user


# ── SESSION / RESULT HELPERS ──────────────────────────────
def detect_session(dt: Optional[datetime]) -> str:
    if not dt: return "UNKNOWN"
    h = dt.hour
    if  7 <= h < 12: return "LONDON"
    if 12 <= h < 17: return "NEW YORK"
    if  0 <= h <  7: return "ASIA"
    return "OFF-HOURS"


def detect_result(profit: float) -> str:
    if profit >  0.5: return "WIN"
    if profit < -0.5: return "LOSS"
    return "BE"


def parse_dt(s: Optional[str]) -> Optional[datetime]:
    if not s: return None
    for fmt in ["%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S",
                "%Y-%m-%dT%H:%M:%S.%f", "%Y.%m.%d %H:%M:%S", "%Y-%m-%d"]:
        try:    return datetime.strptime(s, fmt)
        except: continue
    return None


def trade_dict(t: TradeDB) -> dict:
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


def user_dict(u: UserDB) -> dict:
    return {
        "id":         u.id,
        "username":   u.username,
        "name":       u.name,
        "created_at": str(u.created_at),
        "mt5_login":  u.mt5_login,
        "mt5_server": u.mt5_server,
    }


# ══════════════════════════════════════════════════════════
#  ROUTES
# ══════════════════════════════════════════════════════════

@app.get("/")
def root():
    return {"status": "TradeDesk API v2 running"}


@app.get("/health")
def health(db: Session = Depends(get_db)):
    try:
        db.execute(text("SELECT 1"))
        return {"status": "ok", "database": "connected"}
    except Exception as e:
        return {"status": "error", "database": str(e)}


# ── AUTH ──────────────────────────────────────────────────
class RegisterBody(BaseModel):
    username: str
    name:     str
    password: str


class LoginBody(BaseModel):
    username: str
    password: str


@app.post("/auth/register")
def register(body: RegisterBody, db: Session = Depends(get_db)):
    username = body.username.strip().lower()

    if len(username) < 3:
        raise HTTPException(400, "Username must be at least 3 characters")
    if not all(c.isalnum() or c == "_" for c in username):
        raise HTTPException(400, "Username: only letters, numbers, underscore")
    if len(body.password) < 4:
        raise HTTPException(400, "Password must be at least 4 characters")
    if not body.name.strip():
        raise HTTPException(400, "Name is required")
    if db.query(UserDB).filter(UserDB.username == username).first():
        raise HTTPException(400, "Username already taken")

    user = UserDB(
        username  = username,
        name      = body.name.strip(),
        pass_hash = hash_pw(body.password)
    )
    db.add(user)
    db.commit()
    db.refresh(user)

    return {"token": make_token(user.id), "user": user_dict(user)}


@app.post("/auth/login")
def login(body: LoginBody, db: Session = Depends(get_db)):
    username = body.username.strip().lower()
    user     = db.query(UserDB).filter(UserDB.username == username).first()

    if not user:
        raise HTTPException(401, "Account not found")
    if user.pass_hash != hash_pw(body.password):
        raise HTTPException(401, "Wrong password")

    return {"token": make_token(user.id), "user": user_dict(user)}


@app.get("/auth/me")
def me(user: UserDB = Depends(verify_token)):
    return user_dict(user)


# ── TRADES ────────────────────────────────────────────────
class TradeBody(BaseModel):
    symbol:      str
    direction:   str
    profit:      float
    open_time:   Optional[str]   = None
    close_time:  Optional[str]   = None
    open_price:  Optional[float] = None
    close_price: Optional[float] = None
    sl:          Optional[float] = None
    tp:          Optional[float] = None
    volume:      Optional[float] = None
    setup:       Optional[str]   = None
    notes:       Optional[str]   = None
    emotion:     Optional[int]   = None
    risk:        Optional[float] = None


class UpdateBody(BaseModel):
    notes:   Optional[str]  = None
    emotion: Optional[int]  = None
    setup:   Optional[str]  = None
    result:  Optional[str]  = None
    risk:    Optional[float]= None


@app.get("/trades")
def get_trades(user: UserDB = Depends(verify_token), db: Session = Depends(get_db)):
    rows = db.query(TradeDB)\
             .filter(TradeDB.user_id == user.id)\
             .order_by(TradeDB.open_time.desc())\
             .all()
    return [trade_dict(t) for t in rows]


@app.post("/trades")
def create_trade(body: TradeBody, user: UserDB = Depends(verify_token), db: Session = Depends(get_db)):
    open_dt  = parse_dt(body.open_time)
    close_dt = parse_dt(body.close_time)
    duration = int((close_dt - open_dt).total_seconds() / 60) if open_dt and close_dt else None
    risk     = body.risk or 0

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
        risk         = risk,
        r_multiple   = round(body.profit / risk, 2) if risk else None,
        is_manual    = True
    )
    db.add(trade)
    db.commit()
    db.refresh(trade)
    return trade_dict(trade)


@app.put("/trades/{trade_id}")
def update_trade(trade_id: str, body: UpdateBody,
                 user: UserDB = Depends(verify_token), db: Session = Depends(get_db)):
    t = db.query(TradeDB).filter(TradeDB.id == trade_id, TradeDB.user_id == user.id).first()
    if not t:
        raise HTTPException(404, "Trade not found")
    if body.notes   is not None: t.notes   = body.notes
    if body.emotion is not None: t.emotion = body.emotion
    if body.setup   is not None: t.setup   = body.setup
    if body.result  is not None: t.result  = body.result
    if body.risk    is not None:
        t.risk       = body.risk
        t.r_multiple = round(t.profit / body.risk, 2) if body.risk else None
    db.commit()
    db.refresh(t)
    return trade_dict(t)


@app.delete("/trades/{trade_id}")
def delete_trade(trade_id: str, user: UserDB = Depends(verify_token), db: Session = Depends(get_db)):
    t = db.query(TradeDB).filter(TradeDB.id == trade_id, TradeDB.user_id == user.id).first()
    if not t:
        raise HTTPException(404, "Trade not found")
    db.delete(t)
    db.commit()
    return {"message": "Deleted"}


# ── MT5 SYNC ─────────────────────────────────────────────
class MT5Trade(BaseModel):
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


class SyncBody(BaseModel):
    trades: List[MT5Trade]


@app.post("/trades/sync")
def sync_trades(body: SyncBody, user: UserDB = Depends(verify_token), db: Session = Depends(get_db)):
    added = 0
    updated = 0

    for t in body.trades:
        open_dt  = parse_dt(t.open_time)
        close_dt = parse_dt(t.close_time)
        duration = int((close_dt - open_dt).total_seconds() / 60) if open_dt and close_dt else None

        existing = db.query(TradeDB).filter(
            TradeDB.user_id    == user.id,
            TradeDB.mt5_ticket == t.ticket
        ).first()

        if existing:
            # Update financial data, keep manual fields
            existing.profit       = t.profit
            existing.close_price  = t.close_price
            existing.close_time   = close_dt
            existing.sl           = t.sl
            existing.tp           = t.tp
            existing.swap         = t.swap
            existing.commission   = t.commission
            existing.duration_min = duration
            existing.result       = detect_result(t.profit)
            if existing.risk:
                existing.r_multiple = round(t.profit / existing.risk, 2)
            db.commit()
            updated += 1
        else:
            new = TradeDB(
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
            db.add(new)
            db.commit()
            added += 1

    return {"message": "Sync complete", "added": added, "updated": updated}


# ── STATS ─────────────────────────────────────────────────
@app.get("/stats")
def get_stats(user: UserDB = Depends(verify_token), db: Session = Depends(get_db)):
    trades = db.query(TradeDB).filter(TradeDB.user_id == user.id).all()
    if not trades:
        return {"total": 0, "pnl": 0, "winrate": 0, "profit_factor": 0,
                "expectancy": 0, "max_drawdown": 0, "best_trade": 0,
                "worst_trade": 0, "avg_hold_min": 0,
                "by_setup": {}, "by_session": {}, "by_symbol": {}, "dow_pnl": [0]*7}

    wins   = [t for t in trades if t.result == "WIN"]
    losses = [t for t in trades if t.result == "LOSS"]
    pnl    = sum(t.profit for t in trades)
    gp     = sum(t.profit for t in wins)
    gl     = abs(sum(t.profit for t in losses)) or 1
    pf     = gp / gl if gl > 0 else 0

    eq = 0; peak = 0; dd = 0
    for t in sorted(trades, key=lambda x: x.open_time or datetime.utcnow()):
        eq += t.profit
        if eq > peak: peak = eq
        if peak - eq > dd: dd = peak - eq

    def group(field):
        g = {}
        for t in trades:
            k = getattr(t, field) or "Unknown"
            if k not in g: g[k] = {"wins": 0, "total": 0, "pnl": 0}
            g[k]["total"] += 1
            g[k]["pnl"]   += t.profit
            if t.result == "WIN": g[k]["wins"] += 1
        return g

    dow = [0.0] * 7
    for t in trades:
        if t.open_time:
            dow[t.open_time.weekday()] += t.profit

    avg_hold = sum((t.duration_min or 0) for t in trades) / len(trades)

    return {
        "total":         len(trades),
        "wins":          len(wins),
        "losses":        len(losses),
        "winrate":       round(len(wins) / len(trades) * 100, 1),
        "pnl":           round(pnl, 2),
        "profit_factor": round(pf, 2),
        "expectancy":    round(pnl / len(trades), 2),
        "max_drawdown":  round(dd, 2),
        "best_trade":    round(max(t.profit for t in trades), 2),
        "worst_trade":   round(min(t.profit for t in trades), 2),
        "avg_hold_min":  round(avg_hold, 0),
        "by_setup":      group("setup"),
        "by_session":    group("session"),
        "by_symbol":     group("symbol"),
        "dow_pnl":       [round(x, 2) for x in dow],
    }


# ── EXPORT ────────────────────────────────────────────────
@app.get("/trades/export")
def export_trades(user: UserDB = Depends(verify_token), db: Session = Depends(get_db)):
    trades = db.query(TradeDB).filter(TradeDB.user_id == user.id)\
               .order_by(TradeDB.open_time.desc()).all()
    out = io.StringIO()
    w   = csv.writer(out)
    w.writerow(["Ticket","Symbol","Direction","Open Time","Close Time",
                "Open Price","Close Price","SL","TP","Volume",
                "Profit","Swap","Commission","Duration(min)",
                "Result","Session","Setup","R","Emotion","Notes"])
    for t in trades:
        w.writerow([t.mt5_ticket, t.symbol, t.direction,
                    t.open_time, t.close_time, t.open_price, t.close_price,
                    t.sl, t.tp, t.volume, t.profit, t.swap, t.commission,
                    t.duration_min, t.result, t.session, t.setup,
                    t.r_multiple, t.emotion, t.notes])
    out.seek(0)
    fname = f"tradedesk_{user.username}_{datetime.utcnow().strftime('%Y%m%d')}.csv"
    return StreamingResponse(
        iter([out.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename={fname}"}
    )
