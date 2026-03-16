from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy.pool import QueuePool

from config import DATABASE_URL, DB_POOL_SIZE, DB_MAX_OVERFLOW, DB_POOL_TIMEOUT

# PostgreSQL connection with explicit connection pool settings.
# Without these, the default pool is too small and will exhaust connections
# under moderate concurrency (every background task + every request holds a
# connection simultaneously).
engine = create_engine(
    DATABASE_URL,
    poolclass=QueuePool,
    pool_size=DB_POOL_SIZE,          # steady-state connections kept alive
    max_overflow=DB_MAX_OVERFLOW,    # extra connections allowed under burst
    pool_timeout=DB_POOL_TIMEOUT,    # seconds to wait for a connection
    pool_pre_ping=True,              # drop stale connections automatically
    pool_recycle=1800,               # recycle connections every 30 min
)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

Base = declarative_base()