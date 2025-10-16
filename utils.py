
import logging, sys, time, random, re
from functools import wraps
from pathlib import Path

def setup_logger(logs_dir: Path, name: str = "app", level=logging.INFO) -> logging.Logger:
    logs_dir.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.handlers.clear()
    fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(name)s | %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
    ch = logging.StreamHandler(sys.stdout); ch.setFormatter(fmt); ch.setLevel(level)
    fh = logging.FileHandler(logs_dir / "run.log", encoding="utf-8"); fh.setFormatter(fmt); fh.setLevel(level)
    logger.addHandler(ch); logger.addHandler(fh)
    return logger

def rate_limited(min_s: float, max_s: float):
    def decorator(fn):
        @wraps(fn)
        def wrapper(*args, **kwargs):
            result = fn(*args, **kwargs)
            time.sleep(random.uniform(min_s, max_s))
            return result
        return wrapper
    return decorator

def parse_price(text: str) -> float | None:
    if not text:
        return None
    cleaned = re.sub(r"[^0-9\.\,]", "", text).replace(",", "")
    try: return float(cleaned)
    except ValueError: return None

def safe_get_text(el):
    try: return el.text.strip()
    except Exception: return None
