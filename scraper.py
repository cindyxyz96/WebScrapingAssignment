# scraper.py
from __future__ import annotations

import json
import random
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Iterable

from selenium import webdriver
from selenium.common.exceptions import TimeoutException, WebDriverException
from selenium.webdriver import Chrome
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from urllib.parse import urlencode, urlparse, urlunparse, parse_qsl

# tqdm (soft dependency)
try:
    from tqdm.auto import tqdm
except Exception:  # pragma: no cover
    def tqdm(iterable: Iterable = None, *_, **__):
        return iterable if iterable is not None else []

from config import SETTINGS
from utils import (
    setup_logger,
    rate_limited,
    parse_price,
    safe_get_text,
)

logger = setup_logger(SETTINGS.logs_dir, "scraper")

# ---------- Constants ----------
PRODUCT_HREF_RE = re.compile(
    r"(/product/[^/]+/[A-Z0-9]{6,}(\b|/)|/site/.+?/.*?/p\?skuId=\d+)",
    re.IGNORECASE,
)

CATEGORY_URL = (
    "https://www.bestbuy.com/site/laptop-computers/all-laptops/"
    "pcmcat138500050001.c?id=pcmcat138500050001"
)

LAPTOP_KEYWORDS = ("laptop", "notebook", "macbook", "chromebook", "2-in-1")

EXCLUDED_SECTION_TITLES = (
    "popular laptops",
    "you recently viewed",
    "explore related products",
    "customers also viewed",
    "featured",
)

# ---------- Driver ----------
def _build_driver() -> Chrome:
    opts = Options()
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_experimental_option("excludeSwitches", ["enable-automation"])
    opts.add_experimental_option("useAutomationExtension", False)

    # Fast desktop UA
    opts.add_argument(
        "--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36"
    )
    opts.add_argument("--window-size=1920,1080")
    if getattr(SETTINGS, "headless", True):
        opts.add_argument("--headless=new")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--no-sandbox")

    # Lighter pages: block heavy resources
    opts.add_experimental_option("prefs", {
        "profile.managed_default_content_settings.images": 2,
        "profile.managed_default_content_settings.plugins": 2,
        "profile.managed_default_content_settings.popups": 2,
    })
    opts.set_capability("pageLoadStrategy", "eager")

    if getattr(SETTINGS, "chrome_binary", None):
        opts.binary_location = SETTINGS.chrome_binary

    driver = webdriver.Chrome(options=opts)

    # Hide webdriver flag
    driver.execute_cdp_cmd(
        "Page.addScriptToEvaluateOnNewDocument",
        {"source": "Object.defineProperty(navigator,'webdriver',{get:()=>undefined})"},
    )

    # Block heavy network
    try:
        driver.execute_cdp_cmd("Network.enable", {})
        driver.execute_cdp_cmd("Network.setBypassServiceWorker", {"bypass": True})
        driver.execute_cdp_cmd("Network.setBlockedURLs", {
            "urls": [
                "*.png", "*.jpg", "*.jpeg", "*.gif", "*.webp", "*.svg",
                "*.woff", "*.woff2", "*.ttf", "*.otf",
                "*.mp4", "*.webm", "*.mov", "*.avi",
            ]
        })
    except Exception:
        pass

    driver.implicitly_wait(getattr(SETTINGS, "implicit_wait", 5))
    driver.set_page_load_timeout(getattr(SETTINGS, "page_load_timeout", 60))
    driver.set_script_timeout(getattr(SETTINGS, "script_timeout", 30))
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
        "https://www.bestbuy.com/site/searchpage.jsp?"
        "browsedCategory=pcmcat138500050001&id=pcat17071&"
        "st=categoryid$pcmcat138500050001"
    ),
]

# ---------- Page helpers ----------
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
        driver.add_cookie({"name": "intl_splash", "value": "false",
                           "domain": ".bestbuy.com", "path": "/"})
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
        time.sleep(0.3)
    raise TimeoutException("PLP not ready (no sidebar or product grid found).")


def _wait_for_grid_refresh(driver: Chrome, prev_count: int | None = None, timeout: int = 20) -> int:
    end = time.time() + timeout
    last_seen = -1
    while time.time() < end:
        cards = _find_plp_cards(driver)
        count = len(cards)
        if count > 0 and (prev_count is None or count != prev_count):
            return count
        time.sleep(0.4)
        last_seen = count
    return last_seen


def seed_location(driver: Chrome, zip_code: str = "96939", store_id: str = "852") -> None:
    driver.get("https://www.bestbuy.com/")
    time.sleep(0.8)
    for ck in [
        {"name": "locDestZip", "value": zip_code, "domain": ".bestbuy.com", "path": "/"},
        {"name": "locStoreId", "value": store_id, "domain": ".bestbuy.com", "path": "/"},
    ]:
        try:
            driver.add_cookie(ck)
        except Exception:
            pass


def _wait(driver: Chrome, by, locator, timeout: int | None = None):
    timeout = timeout or getattr(SETTINGS, "explicit_wait", 15)
    return WebDriverWait(driver, timeout).until(EC.presence_of_element_located((by, locator)))


def _try_click_any(driver: Chrome, selectors: list[str], wait_after=0.2) -> bool:
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
                    time.sleep(0.05)
                except Exception:
                    pass
        if blocked:
            try:
                ActionChains(driver).send_keys(Keys.ESCAPE).perform()
                time.sleep(0.05)
            except Exception:
                pass
        for el in driver.find_elements(By.CSS_SELECTOR, "[data-testid='sheet-id-backdrop']"):
            try:
                driver.execute_script("el=arguments[0]; el.remove && el.remove();", el)
            except Exception:
                pass
        if not driver.find_elements(By.CSS_SELECTOR, "[data-testid='sheet-id-backdrop']"):
            break


def smart_scroll(driver: Chrome, passes=6, pause=0.35) -> None:
    body = driver.find_element(By.TAG_NAME, "body")
    for _ in range(passes):
        body.send_keys(Keys.END)
        time.sleep(pause)


def navigate_to_laptops(driver: Chrome) -> None:
    logger.info("Navigating to laptops PLP")
    driver.get("https://www.bestbuy.com/?intl=nosplash")
    time.sleep(0.5)
    try:
        driver.add_cookie({"name": "intl_splash", "value": "false",
                           "domain": ".bestbuy.com", "path": "/"})
        driver.add_cookie({"name": "locDestZip", "value": "96939",
                           "domain": ".bestbuy.com", "path": "/"})
        driver.add_cookie({"name": "locStoreId", "value": "852",
                           "domain": ".bestbuy.com", "path": "/"})
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


# ---------- MAIN RESULTS-ONLY harvesting ----------
def _results_count_hint(driver: Chrome) -> int | None:
    # e.g., "Showing 1-24 of 202 results"
    for sel in [
        "[data-testid='results-summary']",
        "[data-testid='search-results-count']",
        ".results-summary",
        "[data-testid='pagination-summary']",
    ]:
        try:
            txt = (driver.find_element(By.CSS_SELECTOR, sel).text or "").strip()
            m = re.search(r"of\s+(\d[\d,]*)\s+results?", txt, re.I)
            if m:
                return int(m.group(1).replace(",", ""))
        except Exception:
            pass
    return None


def _pagination_buttons(driver: Chrome) -> list[str]:
    """Return list of cp= page numbers visible in the pagination tabs."""
    pages = []
    for sel in [
        "[data-testid='pagination']",
        "nav[aria-label='Pagination']",
        "ul.pagination",
    ]:
        try:
            nav = driver.find_element(By.CSS_SELECTOR, sel)
            btns = nav.find_elements(By.CSS_SELECTOR, "a, button")
            for b in btns:
                t = (b.text or "").strip()
                if t.isdigit():
                    pages.append(t)
        except Exception:
            continue
    # Dedup, keep order
    out = []
    for p in pages:
        if p not in out:
            out.append(p)
    return out


def _find_main_results_containers(driver: Chrome) -> list:
    """
    Identify containers that hold the filtered list results (not carousels or cross-sell sections).
    """
    candidates = []

    # Preferred modern container
    for sel in [
        "[data-testid='list-results']",
        "ol.sku-item-list",
        "div.results-list",
        "[data-testid='sku-list']",
    ]:
        candidates += driver.find_elements(By.CSS_SELECTOR, sel)

    # Filter out containers that belong to excluded sections by nearest heading
    finals = []
    for c in candidates:
        try:
            root = c
            # Walk up a bit and search for a heading nearby (h2/h3/aria)
            parent = c
            heading_txt = ""
            for _ in range(3):
                parent = parent.find_element(By.XPATH, "./..")
                try:
                    h = parent.find_element(By.CSS_SELECTOR, "h2, h3, [aria-label]")
                    heading_txt = (h.text or h.get_attribute("aria-label") or "").strip().lower()
                    if heading_txt:
                        break
                except Exception:
                    continue
            if any(t in heading_txt for t in EXCLUDED_SECTION_TITLES):
                continue
            finals.append(root)
        except Exception:
            finals.append(c)

    # De-duplicate by id
    seen = set()
    uniq = []
    for c in finals:
        ref = c.id if hasattr(c, "id") else id(c)
        if ref not in seen:
            uniq.append(c)
            seen.add(ref)
    return uniq


def _find_plp_cards(driver: Chrome) -> list:
    """
    Find only cards inside the main filtered results container(s).
    """
    containers = _find_main_results_containers(driver)
    cards = []
    for cont in containers:
        try:
            cards += cont.find_elements(By.CSS_SELECTOR, "[data-testid='sku-item'], li.product-list-item.product-list-item-gridView")
        except Exception:
            continue
    return cards


def _extract_product_urls_from_results(driver: Chrome) -> set[str]:
    """
    Extract product URLs from cards within the main results container(s) only.
    """
    urls: set[str] = set()
    for card in _find_plp_cards(driver):
        try:
            a = card.find_element(By.CSS_SELECTOR, "a[href]")
            href = a.get_attribute("href") or ""
            href = href.split("#")[0]
            if "bestbuy.com" in href and PRODUCT_HREF_RE.search(href):
                urls.add(href)
        except Exception:
            # try any child anchors
            try:
                anchors = card.find_elements(By.CSS_SELECTOR, "a[href]")
                for a2 in anchors:
                    href = (a2.get_attribute("href") or "").split("#")[0]
                    if "bestbuy.com" in href and PRODUCT_HREF_RE.search(href):
                        urls.add(href)
                        break
            except Exception:
                continue
    return urls


def _click_show_more_or_next(driver: Chrome) -> bool:
    candidates = [
        "button[data-testid='btn-load-more']",
        "a[aria-label='Next'], button[aria-label='Next']",
        "a[aria-label='Next Page'], button[aria-label='Next Page']",
    ]
    for sel in candidates:
        try:
            btn = WebDriverWait(driver, 2).until(EC.element_to_be_clickable((By.CSS_SELECTOR, sel)))
            driver.execute_script("arguments[0].scrollIntoView({block:'center'});", btn)
            driver.execute_script("arguments[0].click();", btn)
            _ = _wait_for_grid_refresh(driver, timeout=20)
            return True
        except Exception:
            continue
    return False


def _set_query_param(url: str, key: str, val: str | None) -> str:
    parts = list(urlparse(url))
    q = dict(parse_qsl(parts[4]))
    if val is None:
        q.pop(key, None)
    else:
        q[key] = str(val)
    parts[4] = urlencode(q)
    return urlunparse(parts)


def list_products(driver: Chrome) -> list[dict]:
    """
    Harvest ONLY filtered products from the main results, across tabbed pages.
    Returns items and logs page count + total count.
    """
    logger.info("Collecting filtered product links from results (excluding cross-sell)…")
    _wait(driver, By.CSS_SELECTOR, "body")
    clear_overlays(driver)
    _dismiss_backdrops(driver)

    base_url = driver.current_url
    max_pages = max(1, getattr(SETTINGS, "max_pages", 25))
    max_products = max(1, getattr(SETTINGS, "max_products", 800))

    total_hint = _results_count_hint(driver)
    page_tabs = _pagination_buttons(driver)
    known_pages = sorted({int(p) for p in page_tabs}) if page_tabs else [1]

    all_urls: set[str] = set()
    visited_pages = 0

    def harvest_current() -> set[str]:
        # Scroll to ensure all items in this page load
        smart_scroll(driver, passes=8, pause=0.25)
        clear_overlays(driver)
        _dismiss_backdrops(driver)
        return _extract_product_urls_from_results(driver)

    # If we detected page tabs, iterate them; else, try cp= fallback + Show more/Next
    if known_pages and len(known_pages) > 1:
        for cp in known_pages:
            if visited_pages >= max_pages or len(all_urls) >= max_products:
                break
            page_url = _set_query_param(base_url, "cp", str(cp))
            logger.info(f"Open tab page cp={cp}")
            driver.get(page_url)
            try:
                _assert_laptop_plp_loaded(driver, timeout=20)
            except TimeoutException:
                logger.warning("PLP not ready on tab; skipping page.")
                continue
            new = harvest_current()
            before = len(all_urls)
            all_urls |= new
            visited_pages += 1
            logger.info(f"Tab {cp}: +{len(all_urls)-before} (total {len(all_urls)})")
    else:
        # cp= fallback
        cp = 1
        while cp <= max_pages and len(all_urls) < max_products:
            page_url = _set_query_param(base_url, "cp", str(cp)) if cp > 1 else base_url
            if cp > 1:
                logger.info(f"Open page cp={cp}")
                driver.get(page_url)
                try:
                    _assert_laptop_plp_loaded(driver, timeout=20)
                except TimeoutException:
                    logger.warning("PLP not ready via cp=; try Show more/Next.")
                    break
            before = len(all_urls)
            all_urls |= harvest_current()
            visited_pages += 1
            logger.info(f"Page {cp}: +{len(all_urls)-before} (total {len(all_urls)})")

            # If nothing new, try Show more/Next once.
            if len(all_urls) == before:
                if _click_show_more_or_next(driver):
                    all_urls |= harvest_current()
                    logger.info(f"After Show more/Next: total {len(all_urls)}")
                else:
                    break
            if total_hint and len(all_urls) >= total_hint:
                break
            cp += 1

    if len(all_urls) > max_products:
        logger.info(f"Capping URLs at max_products={max_products}")
        all_urls = set(list(all_urls)[:max_products])

    logger.info(f"Detected pages (tabs): {len(known_pages) if page_tabs else visited_pages} | "
                f"Collected items: {len(all_urls)}"
                + (f" | Results hint: {total_hint}" if total_hint else ""))

    items = [{"name": None, "price": None, "rating": None, "reviews": 0, "url": u} for u in sorted(all_urls)]
    return items


# ---------- Filters ----------
def _scroll_into_view(driver: Chrome, element) -> None:
    driver.execute_script("arguments[0].scrollIntoView({block:'center'});", element)
    time.sleep(0.1)


def _click_checkbox_by(driver: Chrome, by, value, desc: str, wait_after=0.8) -> bool:
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
    min_inp = container.find_element(By.CSS_SELECTOR, "input[aria-label='Minimum price'], input[placeholder='Min Price']")
    max_inp = container.find_element(By.CSS_SELECTOR, "input[aria-label='Maximum price'], input[placeholder='Max Price']")
    set_btn = container.find_element(By.CSS_SELECTOR, "button.current-price-facet-set-button")

    _scroll_into_view(driver, min_inp)

    for el in (min_inp, max_inp):
        try: el.clear()
        except Exception: pass

    try: driver.execute_script("arguments[0].focus();", min_inp)
    except Exception: pass
    _react_set_input_value(driver, min_inp, str(min_price))
    time.sleep(0.05)

    try: driver.execute_script("arguments[0].focus();", max_inp)
    except Exception: pass
    _react_set_input_value(driver, max_inp, str(max_price))
    time.sleep(0.05)

    end = time.time() + 6
    while time.time() < end:
        _dismiss_backdrops(driver)
        disabled = set_btn.get_attribute("disabled")
        busy = set_btn.get_attribute("aria-busy")
        cls = set_btn.get_attribute("class") or ""
        if not disabled and busy != "true" and "opacity-disabled" not in cls:
            break
        try:
            driver.execute_script(
                "arguments[0].dispatchEvent(new KeyboardEvent('keydown', {key:'Enter', bubbles:true}));",
                max_inp
            )
        except Exception:
            pass
        time.sleep(0.15)

    try:
        _dismiss_backdrops(driver)
        driver.execute_script("arguments[0].scrollIntoView({block:'center'});", set_btn)
        driver.execute_script("arguments[0].click();", set_btn)
    except Exception:
        try:
            driver.execute_script(
                "arguments[0].dispatchEvent(new KeyboardEvent('keydown', {key:'Enter', bubbles:true}));",
                max_inp
            )
        except Exception:
            pass

    _ = _wait_for_grid_refresh(driver, timeout=25)
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
        wait_after=0.8,
    )
    _wait_for_grid_refresh(driver)
    _dismiss_backdrops(driver)

    # Brand facet expand
    try:
        show_all = driver.find_element(
            By.CSS_SELECTOR,
            "section.facet[data-facet='brand_facet'] button.show-more-link, [data-show-more='brand_facet']"
        )
        _scroll_into_view(driver, show_all)
        _dismiss_backdrops(driver)
        driver.execute_script("arguments[0].click();", show_all)
        time.sleep(0.3)
    except Exception:
        pass

    # Brands
    for b in SETTINGS.top_brands:
        ok = _click_checkbox_by(
            driver,
            By.XPATH,
            f"//section[@data-facet='brand_facet']//input[@id={repr(b)}]",
            f"Brand: {b}",
            wait_after=0.6,
        )
        if not ok:
            _click_checkbox_by(
                driver,
                By.XPATH,
                f"//section[@data-facet='brand_facet']//label[contains(., {repr(b)})]//input[@type='checkbox']",
                f"Brand: {b}",
                wait_after=0.6,
            )
        _wait_for_grid_refresh(driver)
        _dismiss_backdrops(driver)

    # Price
    _set_price_range(driver, SETTINGS.price_min, SETTINGS.price_max)


# ---------- PDP scraping ----------
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
        ".priceView-hero-price span",
    ]:
        try:
            price_text = safe_get_text(driver.find_element(By.CSS_SELECTOR, sel))
            if price_text:
                break
        except Exception:
            pass
    price = parse_price(price_text or "")

    # Rating (best-effort)
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


def _is_laptop_category(driver: Chrome) -> bool:
    """
    Validate PDP belongs to laptops using breadcrumb/category cues and title.
    """
    checks = [
        "[data-testid='breadcrumb']",
        "nav.breadcrumb",
        "ol[aria-label='breadcrumb']",
        ".shop-breadcrumb",
    ]
    for sel in checks:
        try:
            txt = (driver.find_element(By.CSS_SELECTOR, sel).text or "").lower()
            if any(kw in txt for kw in ("laptop", "macbook", "notebook", "computers")):
                return True
        except Exception:
            continue
    try:
        title = (driver.title or "").lower()
        if any(kw in title for kw in LAPTOP_KEYWORDS):
            return True
    except Exception:
        pass
    return False


def _stable_get(driver: Chrome, url: str, timeout: int) -> None:
    try:
        driver.set_page_load_timeout(timeout)
        driver.get(url)
    except TimeoutException:
        try:
            WebDriverWait(driver, 5).until(EC.presence_of_element_located((By.CSS_SELECTOR, "body")))
            logger.debug("Continuing after load timeout: DOM is present.")
        except Exception as e:
            raise e


@rate_limited(getattr(SETTINGS, "rate_limit_min_s", 0.0), getattr(SETTINGS, "rate_limit_max_s", 0.0))
def fetch_product_detail(url: str, attempt: int = 1, max_attempts: int = 2) -> dict:
    driver: Chrome | None = None
    try:
        driver = _build_driver()
        _stable_get(driver, url, timeout=getattr(SETTINGS, "page_load_timeout", 60))
        _wait(driver, By.CSS_SELECTOR, "body", timeout=15)
        clear_overlays(driver)
        _dismiss_backdrops(driver)

        if not _is_laptop_category(driver):
            logger.warning(f"Non-laptop PDP (skipped): {url}")
            return {"skip": True, "url": url, "name": None, "price": None, "rating": None, "specs": {}, "reviews": []}

        name, price, rating = _extract_name_price_rating_on_pdp(driver)

        specs: dict[str, Any] = {}
        for row in driver.find_elements(By.CSS_SELECTOR, ".specification-row, .specs tr, [data-testid*='spec'] tr"):
            try:
                k = safe_get_text(row.find_element(By.CSS_SELECTOR, ".row-title, th, [data-testid='spec-key']"))
                v = safe_get_text(row.find_element(By.CSS_SELECTOR, ".row-value, td, [data-testid='spec-value']"))
                if k:
                    specs[k] = v
            except Exception:
                continue

        # Reviews: optional, best-effort
        reviews: list[dict[str, Any]] = []
        for r in driver.find_elements(By.CSS_SELECTOR, ".review, .ugc-review, [data-testid*='review']"):
            try:
                txt = safe_get_text(
                    r.find_element(By.CSS_SELECTOR, ".pre-white-space, .review-text, [data-testid='review-text']")
                )
                score_txt = safe_get_text(
                    r.find_element(By.CSS_SELECTOR, ".c-review-average, .rating, [data-testid='rating']")
                )
                m = re.search(r"(\d+(\.\d+)?)", score_txt or "")
                score = float(m.group(1)) if m else None
                reviews.append({"text": txt, "score": score})
            except Exception:
                continue

        return {"skip": False, "url": url, "name": name, "price": price, "rating": rating, "specs": specs, "reviews": reviews}

    except WebDriverException as e:
        logger.error(f"Selenium error on {url}: {e}")
        if attempt < max_attempts:
            time.sleep(0.5 + random.random())
            return fetch_product_detail(url, attempt=attempt + 1, max_attempts=max_attempts)
        return {"skip": True, "url": url, "name": None, "price": None, "rating": None, "specs": {}, "reviews": []}
    finally:
        if driver:
            try:
                driver.quit()
            except Exception:
                pass


# ---------- Enrichment ----------
def enrich_products(products: list[dict]) -> list[dict]:
    """
    Enrich product seeds with PDP details.
    Multithreaded with tqdm; guarded by a semaphore to avoid Chrome overload.
    """
    if not products:
        return []

    total = len(products)
    max_workers = max(1, getattr(SETTINGS, "threads", 6))
    enable_mt = bool(getattr(SETTINGS, "enable_multithreading", True))
    max_parallel = max(1, getattr(SETTINGS, "max_parallel_browsers", min(6, max_workers)))

    if enable_mt and max_workers > 1:
        from threading import Semaphore
        gate = Semaphore(max_parallel)

        def guarded_fetch(u: str) -> dict:
            with gate:
                return fetch_product_detail(u)

        with ThreadPoolExecutor(max_workers=max_workers) as ex, \
             tqdm(total=total, desc="Fetching details", unit="item", dynamic_ncols=True) as pbar:

            fut_to_idx = {ex.submit(guarded_fetch, p["url"]): i for i, p in enumerate(products)}
            for fut in as_completed(fut_to_idx):
                idx = fut_to_idx[fut]
                try:
                    detail = fut.result()
                    if detail.get("skip"):
                        products[idx]["skip"] = True
                    else:
                        for k, v in detail.items():
                            if k != "skip":
                                products[idx][k] = v
                        products[idx]["reviews"] = len(products[idx].get("reviews") or [])
                except Exception as e:
                    logger.error(f"Failed to enrich product {products[idx].get('url')}: {e}")
                    products[idx]["skip"] = True
                finally:
                    pbar.update(1)
    else:
        with tqdm(total=total, desc="Fetching details", unit="item", dynamic_ncols=True) as pbar:
            for p in products:
                try:
                    detail = fetch_product_detail(p["url"])
                    if detail.get("skip"):
                        p["skip"] = True
                    else:
                        for k, v in detail.items():
                            if k != "skip":
                                p[k] = v
                        p["reviews"] = len(p.get("reviews") or [])
                except Exception as e:
                    logger.error(f"Failed to enrich product {p.get('url')}: {e}")
                    p["skip"] = True
                finally:
                    pbar.update(1)

    kept = [p for p in products if not p.get("skip")]
    dropped = len(products) - len(kept)
    if dropped:
        logger.info(f"Filtered out {dropped} non-laptop/failed PDPs.")
    return kept


# ---------- Entry ----------
def run_scrape(output_json: Path | None = None) -> list[dict]:
    """
    Orchestrates: navigate → apply filters → collect only filtered results across tabbed pages
    → PDP enrich → save JSON, and logs page & item counts.
    """
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
