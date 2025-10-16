# scraper.py
from __future__ import annotations

import json
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Iterable, Optional
from urllib.parse import urlencode, urlparse, urlunparse, parse_qsl

from selenium import webdriver
from selenium.common.exceptions import (
    TimeoutException,
    WebDriverException,
)
from selenium.webdriver import Chrome
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

# tqdm (fallback graceful)
try:
    from tqdm.auto import tqdm
except Exception:  # pragma: no cover
    def tqdm(iterable: Optional[Iterable] = None, *_, **__):
        return iterable if iterable is not None else []

from config import SETTINGS
from utils import (
    setup_logger,
    rate_limited,
    parse_price,
    safe_get_text,
)

logger = setup_logger(SETTINGS.logs_dir, "scraper")

# PDP link patterns we consider valid
PRODUCT_HREF_RE = re.compile(
    r"(/product/[^/]+/[A-Z0-9]{6,}"
    r"|/site/.+?/.*?/p\?skuId=\d+"
    r"|/site/[-a-z0-9]+/\d+\.p)",  # some “/site/<slug>/<sku>.p” variants
    re.IGNORECASE,
)

CATEGORY_URL = (
    "https://www.bestbuy.com/site/laptop-computers/all-laptops/pcmcat138500050001.c"
    "?id=pcmcat138500050001"
)


# =========================
# Driver setup (fast & light)
# =========================
def _build_driver() -> Chrome:
    opts = Options()
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_experimental_option("excludeSwitches", ["enable-automation"])
    opts.add_experimental_option("useAutomationExtension", False)
    opts.add_argument(
        "--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36"
    )
    opts.add_argument("--window-size=1920,1080")
    if SETTINGS.headless:
        opts.add_argument("--headless=new")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--no-sandbox")
    # Make load snappier
    opts.set_capability("pageLoadStrategy", "eager")

    # Block heavy assets for speed (do NOT block .svg as some UIs rely on it)
    opts.add_experimental_option(
        "prefs",
        {
            "profile.managed_default_content_settings.images": 2,
            "profile.managed_default_content_settings.plugins": 2,
            "profile.managed_default_content_settings.popups": 2,
        },
    )

    if getattr(SETTINGS, "chrome_binary", None):
        opts.binary_location = SETTINGS.chrome_binary

    driver = webdriver.Chrome(options=opts)

    # Hide webdriver flag
    driver.execute_cdp_cmd(
        "Page.addScriptToEvaluateOnNewDocument",
        {"source": "Object.defineProperty(navigator,'webdriver',{get:()=>undefined})"},
    )

    # Block heavy network at protocol level too
    try:
        driver.execute_cdp_cmd("Network.enable", {})
        driver.execute_cdp_cmd("Network.setBypassServiceWorker", {"bypass": True})
        driver.execute_cdp_cmd(
            "Network.setBlockedURLs",
            {
                "urls": [
                    "*.png",
                    "*.jpg",
                    "*.jpeg",
                    "*.gif",
                    "*.webp",
                    # keep "*.svg" allowed
                    "*.woff",
                    "*.woff2",
                    "*.ttf",
                    "*.otf",
                    "*.mp4",
                    "*.webm",
                    "*.mov",
                    "*.avi",
                ]
            },
        )
    except Exception:
        pass

    driver.implicitly_wait(getattr(SETTINGS, "implicit_wait", 5))
    driver.set_page_load_timeout(getattr(SETTINGS, "page_load_timeout", 60))
    return driver


def _add_nosplash(url: str) -> str:
    parts = list(urlparse(url))
    q = dict(parse_qsl(parts[4]))
    q["intl"] = "nosplash"
    parts[4] = urlencode(q)
    return urlunparse(parts)


PLP_URLS = [
    _add_nosplash(CATEGORY_URL),
    _add_nosplash(
        "https://www.bestbuy.com/site/searchpage.jsp?browsedCategory=pcmcat138500050001"
        "&id=pcat17071&st=categoryid$pcmcat138500050001"
    ),
]


# =========================
# Splash / navigation helpers
# =========================
def _is_country_splash(driver: Chrome) -> bool:
    try:
        return bool(driver.find_elements(By.CSS_SELECTOR, ".country-selection .us-link"))
    except Exception:
        return False


def handle_country_splash(driver: Chrome) -> None:
    if not _is_country_splash(driver):
        return
    logger.info("International splash detected — selecting United States.")
    try:
        us = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, ".country-selection .us-link"))
        )
        driver.add_cookie(
            {"name": "intl_splash", "value": "false", "domain": ".bestbuy.com", "path": "/"}
        )
        driver.execute_script("arguments[0].click();", us)
        WebDriverWait(driver, 15).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "body"))
        )
    except Exception as e:
        logger.warning(f"Could not auto-select US on splash: {e}")


def _assert_laptop_plp_loaded(driver: Chrome, timeout: int = 30) -> None:
    end = time.time() + timeout
    while time.time() < end:
        if _is_country_splash(driver):
            return
        for sel in [
            ".sidebar-container",
            "section.facet[data-facet='brand_facet']",
            "section.facet[data-facet='customerreviews_facet']",
            "section.facet[data-facet='currentprice_facet']",
            "[data-testid='sku-item']",
            "li.product-list-item.product-list-item-gridView",
        ]:
            if driver.find_elements(By.CSS_SELECTOR, sel):
                return
        time.sleep(0.5)
    raise TimeoutException("PLP not ready (no sidebar or product grid found).")


def _wait_for_grid_refresh(driver: Chrome, prev_count: int | None = None, timeout: int = 20) -> int:
    end = time.time() + timeout
    last_seen = -1
    while time.time() < end:
        cards = driver.find_elements(
            By.CSS_SELECTOR,
            "[data-testid='sku-item'], li.product-list-item.product-list-item-gridView, "
            "article[data-sku-id]",
        )
        count = len(cards)
        if count > 0 and (prev_count is None or count != prev_count):
            return count
        time.sleep(0.5)
        last_seen = count
    return last_seen


def seed_location(driver: Chrome, zip_code: str = "96939", store_id: str = "852") -> None:
    driver.get("https://www.bestbuy.com/")
    time.sleep(1.0)
    for ck in [
        {"name": "locDestZip", "value": zip_code, "domain": ".bestbuy.com", "path": "/"},
        {"name": "locStoreId", "value": store_id, "domain": ".bestbuy.com", "path": "/"},
    ]:
        try:
            driver.add_cookie(ck)
        except Exception:
            pass


def _wait(driver: Chrome, by, locator, timeout: int | None = None):
    timeout = timeout or getattr(SETTINGS, "explicit_wait", 12)
    return WebDriverWait(driver, timeout).until(
        EC.presence_of_element_located((by, locator))
    )


def _try_click_any(driver: Chrome, selectors: list[str], wait_after=0.4) -> bool:
    for sel in selectors:
        try:
            el = WebDriverWait(driver, 3).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, sel))
            )
            driver.execute_script("arguments[0].click();", el)
            time.sleep(wait_after)
            return True
        except Exception:
            continue
    return False


def clear_overlays(driver: Chrome) -> None:
    _try_click_any(
        driver,
        [
            ".c-close-icon.c-modal-close-icon",
            "button#lam-signin-close",
            "button[aria-label='Close']",
            "button#close",
            "button[id*='cookie']",
            "button[aria-label*='Accept Cookies']",
            "[data-track='Accept Cookies']",
        ],
    )


def _dismiss_backdrops(driver: Chrome, attempts: int = 4) -> None:
    for _ in range(attempts):
        blocked = False
        selectors = [
            "[data-testid='sheet-id-backdrop']",
            ".modal-backdrop, .c-overlay, .MuiBackdrop-root, .ReactModal__Overlay",
            "div[role='presentation'][style*='background-color']",
            "div[style*='position: fixed'][style*='background-color']",
        ]
        for sel in selectors:
            for el in driver.find_elements(By.CSS_SELECTOR, sel):
                try:
                    blocked = True
                    driver.execute_script("arguments[0].click();", el)
                    time.sleep(0.08)
                except Exception:
                    pass
        if blocked:
            try:
                ActionChains(driver).send_keys(Keys.ESCAPE).perform()
                time.sleep(0.08)
            except Exception:
                pass
        # Hard remove stubborn overlay
        for el in driver.find_elements(By.CSS_SELECTOR, "[data-testid='sheet-id-backdrop']"):
            try:
                driver.execute_script(
                    "el=arguments[0]; el.parentNode && el.parentNode.removeChild(el);", el
                )
                time.sleep(0.05)
            except Exception:
                pass
        try:
            driver.execute_script(
                "document.body.style.overflow='auto'; document.documentElement.style.overflow='auto';"
            )
        except Exception:
            pass
        if not driver.find_elements(By.CSS_SELECTOR, "[data-testid='sheet-id-backdrop']"):
            break


def smart_scroll(driver: Chrome, passes=6, pause=0.5) -> None:
    body = driver.find_element(By.TAG_NAME, "body")
    for _ in range(passes):
        body.send_keys(Keys.END)
        time.sleep(pause)


def navigate_to_laptops(driver: Chrome) -> None:
    logger.info("Navigating to laptops PLP")
    driver.get("https://www.bestbuy.com/?intl=nosplash")
    time.sleep(0.8)
    try:
        driver.add_cookie(
            {"name": "intl_splash", "value": "false", "domain": ".bestbuy.com", "path": "/"}
        )
        driver.add_cookie(
            {"name": "locDestZip", "value": "96939", "domain": ".bestbuy.com", "path": "/"}
        )
        driver.add_cookie(
            {"name": "locStoreId", "value": "852", "domain": ".bestbuy.com", "path": "/"}
        )
    except Exception:
        pass

    for i, url in enumerate(PLP_URLS, 1):
        url = _add_nosplash(url)
        logger.info(f"Navigate attempt {i}/{len(PLP_URLS)}: {url}")
        driver.get(url)
        try:
            _assert_laptop_plp_loaded(driver, timeout=25)
            if _is_country_splash(driver):
                handle_country_splash(driver)
                _assert_laptop_plp_loaded(driver, timeout=25)
            logger.info(f"Arrived at: {driver.current_url}")
            logger.info(f"Title: {driver.title}")
            return
        except TimeoutException as te:
            logger.warning(f"PLP not ready on attempt {i}: {te}")
            continue
    raise TimeoutException("Could not load BestBuy laptops PLP (blocked or template mismatch).")


# =========================
# Results container & tab switch
# =========================
def _switch_to_products_tab(driver: Chrome, timeout=12) -> None:
    # If results already visible, skip
    for sel in ['[data-testid="sku-list"]', 'ol.sku-item-list', '[data-widget="product-list"]', '#search-results-list']:
        if driver.find_elements(By.CSS_SELECTOR, sel):
            return

    WebDriverWait(driver, timeout).until(
        EC.presence_of_element_located((By.CSS_SELECTOR, '[role="tablist"]'))
    )
    tabs = driver.find_elements(By.CSS_SELECTOR, '[role="tab"]')
    target = None
    for t in tabs:
        label = (t.text or t.get_attribute("aria-label") or "").strip().lower()
        if "product" in label:  # “Products”
            target = t
            break
    if target:
        driver.execute_script("arguments[0].click();", target)
    # Wait for a results container (the actual grid)
    WebDriverWait(driver, timeout).until(
        EC.presence_of_element_located((
            By.CSS_SELECTOR,
            '[data-testid="sku-list"], ol.sku-item-list, [data-widget="product-list"], #search-results-list'
        ))
    )


def _get_results_container(driver: Chrome):
    # Preferred selectors for the results (not cross-sell)
    for sel in [
        '[data-testid="sku-list"]',
        'ol.sku-item-list',
        '[data-zone="searchResults"]',
        '[data-widget="product-list"]',
        '#search-results-list',
        'main [data-component="search-results"]',
    ]:
        els = driver.find_elements(By.CSS_SELECTOR, sel)
        if els:
            return els[0]
    # Fallback: main
    return driver.find_element(By.CSS_SELECTOR, 'main')


def _find_product_cards(container) -> list:
    # Keep this broad to handle multiple A/B variants
    cards = container.find_elements(By.CSS_SELECTOR, """
        li.sku-item,
        [data-testid="sku-item"],
        article[data-sku-id]
    """)
    return cards


def _card_to_url(card) -> Optional[str]:
    # Prefer PDP anchors with skuId
    for sel in ['a[href*="/site/"][href*="skuId="]', 'a[href*="/product/"]', 'a[href$=".p"]']:
        a = card.find_elements(By.CSS_SELECTOR, sel)
        if a:
            href = (a[0].get_attribute("href") or "").split("#")[0]
            if PRODUCT_HREF_RE.search(href):
                return href
    # Fallback: use data-sku-id to construct
    sku = (card.get_attribute("data-sku-id") or "").strip()
    if sku:
        return f"https://www.bestbuy.com/site/searchpage.jsp?st={sku}"
    return None


def _results_count_text(driver: Chrome) -> Optional[str]:
    # Best-effort capture of "XXX items" badge
    for sel in [
        "[data-testid='result-count']",
        "h1 ~ span",  # sometimes shows near heading
        "[class*='result-count']",
    ]:
        try:
            txt = (driver.find_element(By.CSS_SELECTOR, sel).text or "").strip()
            if re.search(r"\d+\s+item", txt.lower()):
                return txt
        except Exception:
            continue
    return None


# =========================
# Filtering
# =========================
def _scroll_into_view(driver: Chrome, element) -> None:
    driver.execute_script("arguments[0].scrollIntoView({block:'center'});", element)
    time.sleep(0.15)


def _click_checkbox_by(driver: Chrome, by, value, desc: str, wait_after=1.0) -> bool:
    try:
        el = WebDriverWait(driver, 8).until(EC.presence_of_element_located((by, value)))
        _scroll_into_view(driver, el)
        _dismiss_backdrops(driver)
        driver.execute_script("arguments[0].click();", el)
        time.sleep(wait_after)
        logger.info(f"Applied filter: {desc}")
        return True
    except Exception as e:
        logger.warning(f"Could not apply filter ({desc}): {e}")
        return False


def _react_set_input_value(driver: Chrome, el, value: str) -> None:
    driver.execute_script(
        """
        (function(el, val){
          const proto = window.HTMLInputElement.prototype;
          const desc  = Object.getOwnPropertyDescriptor(proto, 'value');
          if (desc && desc.set) { desc.set.call(el, val); } else { el.value = val; }
          const evts = ['input','change','keyup','blur'];
          for (const t of evts) el.dispatchEvent(new Event(t, {bubbles:true}));
        })(arguments[0], arguments[1]);
        """,
        el, str(value)
    )


def _set_price_range(driver: Chrome, min_price: int, max_price: int) -> None:
    _dismiss_backdrops(driver)
    container = WebDriverWait(driver, 15).until(
        EC.presence_of_element_located((By.CSS_SELECTOR, "section.facet[data-facet='currentprice_facet']"))
    )
    min_inp = container.find_element(
        By.CSS_SELECTOR, "input[aria-label='Minimum price'], input[placeholder='Min Price']"
    )
    max_inp = container.find_element(
        By.CSS_SELECTOR, "input[aria-label='Maximum price'], input[placeholder='Max Price']"
    )
    set_btn = container.find_element(By.CSS_SELECTOR, "button.current-price-facet-set-button")

    try:
        _scroll_into_view(driver, min_inp)
    except Exception:
        pass

    for el in (min_inp, max_inp):
        try:
            el.clear()
        except Exception:
            pass

    # Focus via JS (avoid click interception)
    try:
        driver.execute_script("arguments[0].focus();", min_inp)
    except Exception:
        pass
    _react_set_input_value(driver, min_inp, str(min_price))
    time.sleep(0.1)

    try:
        driver.execute_script("arguments[0].focus();", max_inp)
    except Exception:
        pass
    _react_set_input_value(driver, max_inp, str(max_price))
    time.sleep(0.15)

    # Wait for Set button to be enabled; nudge validation if needed
    end = time.time() + 6
    while time.time() < end:
        _dismiss_backdrops(driver)
        disabled_attr = set_btn.get_attribute("disabled")
        aria_busy = set_btn.get_attribute("aria-busy")
        cls = set_btn.get_attribute("class") or ""
        if not disabled_attr and aria_busy != "true" and "opacity-disabled" not in cls:
            break
        try:
            driver.execute_script(
                "arguments[0].dispatchEvent(new KeyboardEvent('keydown', {key:'Enter', bubbles:true}));",
                max_inp,
            )
        except Exception:
            pass
        time.sleep(0.25)

    # Prefer JS click
    try:
        _dismiss_backdrops(driver)
        driver.execute_script("arguments[0].scrollIntoView({block:'center'});", set_btn)
        driver.execute_script("arguments[0].click();", set_btn)
    except Exception:
        try:
            driver.execute_script(
                "arguments[0].dispatchEvent(new KeyboardEvent('keydown', {key:'Enter', bubbles:true}));",
                max_inp,
            )
        except Exception:
            pass

    count_after = _wait_for_grid_refresh(driver, timeout=25)
    if count_after <= 0:
        logger.warning("Price inputs submitted but no products visible yet; continuing.")
    logger.info(f"Applied filter: Price range ${min_price}–${max_price} via inputs")


def apply_filters(driver: Chrome) -> None:
    logger.info(
        f"Applying filters... (price ${SETTINGS.price_min}–${SETTINGS.price_max}, "
        f"brands {', '.join(SETTINGS.top_brands)}, rating {SETTINGS.min_rating}+)"
    )
    WebDriverWait(driver, 12).until(
        EC.presence_of_element_located((By.CSS_SELECTOR, ".sidebar-container"))
    )
    _dismiss_backdrops(driver)

    # Rating
    rating_label = f"{int(SETTINGS.min_rating)} & Up"
    _click_checkbox_by(
        driver,
        By.XPATH,
        f"//input[@id='customer-rating-{int(SETTINGS.min_rating)}_&_Up' "
        f"or @aria-label='{rating_label}' or @aria-label='{int(SETTINGS.min_rating)} stars & Up']",
        f"Rating: {rating_label}",
        wait_after=1.0,
    )
    _wait_for_grid_refresh(driver)
    _dismiss_backdrops(driver)

    # Expand brands if available
    try:
        show_all = driver.find_element(
            By.CSS_SELECTOR,
            "section.facet[data-facet='brand_facet'] button.show-more-link, [data-show-more='brand_facet']",
        )
        _scroll_into_view(driver, show_all)
        _dismiss_backdrops(driver)
        driver.execute_script("arguments[0].click();", show_all)
        time.sleep(0.5)
    except Exception:
        pass

    for b in SETTINGS.top_brands:
        ok = _click_checkbox_by(
            driver,
            By.XPATH,
            f"//section[@data-facet='brand_facet']//input[@id={repr(b)}]",
            f"Brand: {b}",
            wait_after=0.8,
        )
        if not ok:
            _click_checkbox_by(
                driver,
                By.XPATH,
                f"//section[@data-facet='brand_facet']//label[contains(., {repr(b)})]//input[@type='checkbox']",
                f"Brand: {b}",
                wait_after=0.8,
            )
        _wait_for_grid_refresh(driver)
        _dismiss_backdrops(driver)

    # Price via inputs
    _set_price_range(driver, SETTINGS.price_min, SETTINGS.price_max)

    # Switch to the “Products” tab before collecting results
    _switch_to_products_tab(driver, timeout=12)
    # Log result count if visible
    rc = _results_count_text(driver)
    if rc:
        logger.info(f"Results badge: {rc}")


# =========================
# Pagination utilities
# =========================
def _set_page(url: str, page: int) -> str:
    parts = list(urlparse(url))
    q = dict(parse_qsl(parts[4]))
    q["cp"] = str(page)
    parts[4] = urlencode(q)
    return urlunparse(parts)


def _iterate_pages(driver: Chrome, start_url: str, max_pages: int = 20):
    """
    Yield (page_number, cards_list) across paginated result pages.
    Uses &cp= param; falls back to visible "Next" if present.
    """
    page = 1
    while page <= max_pages:
        if page > 1:
            driver.get(_set_page(start_url, page))
            WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.CSS_SELECTOR, "body")))
            _switch_to_products_tab(driver, timeout=10)

        container = _get_results_container(driver)
        cards = _find_product_cards(container)
        yield page, cards

        # Detect “Next”
        next_btns = driver.find_elements(
            By.CSS_SELECTOR,
            'nav[aria-label="Pagination"] a[aria-label*="Next"], '
            'nav[aria-label="Pagination"] button[aria-label*="Next"]'
        )
        if not next_btns:
            break
        cls = (next_btns[0].get_attribute("class") or "").lower()
        disabled = "disabled" in cls or next_btns[0].get_attribute("aria-disabled") == "true"
        if disabled:
            break
        page += 1


# =========================
# Listing & PDP enrichment
# =========================
def list_products(driver: Chrome) -> list[dict]:
    """
    Collect only filtered product links from the Products tab across all pages.
    Excludes cross-sell blocks.
    """
    logger.info("Collecting filtered product links from results (excluding cross-sell)…")
    _switch_to_products_tab(driver, timeout=12)

    collected: list[str] = []
    seen: set[str] = set()
    start_url = driver.current_url
    total_pages = 0

    max_pages = int(getattr(SETTINGS, "max_pages", 20))

    for page, cards in _iterate_pages(driver, start_url, max_pages=max_pages):
        urls_this_page: list[str] = []
        for c in cards:
            u = _card_to_url(c)
            if not u:
                continue
            if PRODUCT_HREF_RE.search(u):
                urls_this_page.append(u)

        # de-dup while preserving order
        added = 0
        for u in urls_this_page:
            if u not in seen:
                seen.add(u)
                collected.append(u)
                added += 1

        logger.info(f"Page {page}: +{added} (total {len(collected)})")
        total_pages = page

    logger.info(f"Detected pages (tabs): {total_pages} | Collected items: {len(collected)}")

    return [{"name": None, "price": None, "rating": None, "reviews": 0, "url": u} for u in collected]


def _extract_name_price_rating_on_pdp(driver: Chrome) -> tuple[str | None, float | None, float | None]:
    # Name
    name = None
    for sel in ["h1", "[data-testid='heading'] h1", "[data-lu-target='product-title']"]:
        try:
            t = safe_get_text(driver.find_element(By.CSS_SELECTOR, sel))
            if t:
                name = t
                break
        except Exception:
            pass

    # Price
    price_text = None
    for sel in [
        "[data-testid='price-block-customer-price'] span",
        "[data-testid='price-block'] [data-testid*='customer'] span",
        "[data-lu-target='customer_price'] span",
        "[data-lu-target='customer_price']",
        "[data-testid='customer-price'] span"
    ]:
        try:
            price_text = safe_get_text(driver.find_element(By.CSS_SELECTOR, sel))
            if price_text:
                break
        except Exception:
            pass
    price = parse_price(price_text or "")

    # Rating
    rating = None
    for sel in ["[data-testid='rating-stars']", "[aria-label*='rating']", "[data-lu-target='rating']"]:
        try:
            txt = safe_get_text(driver.find_element(By.CSS_SELECTOR, sel))
            m = re.search(r"(\d+(\.\d+)?)", txt or "")
            if m:
                rating = float(m.group(1))
                break
        except Exception:
            pass

    return name, price, rating


@rate_limited(SETTINGS.rate_limit_min_s, SETTINGS.rate_limit_max_s)
def fetch_product_detail(url: str, attempt: int = 1) -> dict:
    """
    Navigate a single PDP and collect specs, price, rating.
    Retries once on page timeouts/renderer hiccups.
    """
    driver: Chrome | None = None
    try:
        driver = _build_driver()
        # Make PDP loads a bit stricter
        driver.set_page_load_timeout(min(getattr(SETTINGS, "page_load_timeout", 60), 45))
        driver.get(url)
        _wait(driver, By.CSS_SELECTOR, "body", timeout=15)
        clear_overlays(driver)
        _dismiss_backdrops(driver)

        name, price, rating = _extract_name_price_rating_on_pdp(driver)

        specs: dict[str, Any] = {}
        for row in driver.find_elements(
            By.CSS_SELECTOR,
            ".specification-row, .specs tr, [data-testid*='spec'] tr, "
            "[data-component='specifications'] tr"
        ):
            try:
                k = safe_get_text(
                    row.find_element(By.CSS_SELECTOR, ".row-title, th, [data-testid='spec-key']")
                )
                v = safe_get_text(
                    row.find_element(By.CSS_SELECTOR, ".row-value, td, [data-testid='spec-value']")
                )
                if k:
                    specs[k] = v
            except Exception:
                continue

        # BestBuy reviews are often lazy/iframes; skip heavy review crawling for speed
        reviews: list[dict[str, Any]] = []

        return {"name": name, "price": price, "rating": rating, "specs": specs, "reviews": reviews}

    except (TimeoutException, WebDriverException) as e:
        if attempt == 1:
            logger.warning(f"PDP timeout, retrying once: {url}")
            try:
                driver.quit()
            except Exception:
                pass
            return fetch_product_detail(url, attempt=2)
        logger.error(f"Selenium error on {url}: {e}")
        return {"name": None, "price": None, "rating": None, "specs": {}, "reviews": []}
    finally:
        if driver:
            try:
                driver.quit()
            except Exception:
                pass


def enrich_products(products: list[dict]) -> list[dict]:
    """
    Enrich product seeds with PDP details.
    Shows a tqdm progress bar for both single-threaded and multithreaded modes.
    """
    if not products:
        return []

    total = len(products)
    if SETTINGS.enable_multithreading and total > 1:
        with ThreadPoolExecutor(max_workers=SETTINGS.threads) as ex, tqdm(
            total=total, desc="Fetching details", unit="item", dynamic_ncols=True
        ) as pbar:
            future_to_idx = {
                ex.submit(fetch_product_detail, p["url"]): i for i, p in enumerate(products)
            }
            for future in as_completed(future_to_idx):
                idx = future_to_idx[future]
                try:
                    detail = future.result()
                    for k, v in detail.items():
                        products[idx][k] = v
                    products[idx]["reviews"] = len(products[idx].get("reviews") or [])
                except Exception as e:
                    logger.error(f"Failed to enrich product {products[idx].get('url')}: {e}")
                finally:
                    pbar.update(1)
        return products
    else:
        with tqdm(total=total, desc="Fetching details", unit="item", dynamic_ncols=True) as pbar:
            for i, p in enumerate(products):
                try:
                    detail = fetch_product_detail(p["url"])
                    for k, v in detail.items():
                        p[k] = v
                    p["reviews"] = len(p.get("reviews") or [])
                except Exception as e:
                    logger.error(f"Failed to enrich product {p.get('url')}: {e}")
                finally:
                    pbar.update(1)
        return products


# =========================
# Entry point
# =========================
def run_scrape(output_json: Path | None = None) -> list[dict]:
    output_json = output_json or SETTINGS.raw_json_path
    driver = _build_driver()
    try:
        seed_location(driver)
        navigate_to_laptops(driver)
        apply_filters(driver)
        products = list_products(driver)
    finally:
        try:
            driver.quit()
        except Exception:
            pass

    logger.info("Fetching detail pages...")
    products = enrich_products(products)

    output_json.parent.mkdir(parents=True, exist_ok=True)
    output_json.write_text(json.dumps(products, indent=2), encoding="utf-8")
    logger.info(f"Wrote raw JSON: {output_json} (items={len(products)})")
    return products
