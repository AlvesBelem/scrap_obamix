import re
import time
from typing import Any, Callable, Dict, List, Optional, Tuple

from selenium.common.exceptions import (
    ElementClickInterceptedException,
    NoSuchElementException,
    StaleElementReferenceException,
)
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

from config.settings import SCRAPER_PAGE_DELAY, SCRAPER_ROW_DELAY
from scraper.modal_scraper import extract_modal_data

ROWS_SELECTOR = "#DataTables_Table_0 tbody tr"
TABLE_SPINNER_SELECTOR = ".loadding-table"
MODAL_TRIGGER_SELECTOR = "td:nth-child(8) a#btnViewProduct"
INT32_MAX = 2_147_483_647
QTY_MAX = 1_000_000
_QUANTITY_PATTERN = re.compile(r"\d{1,3}(?:[.\s]\d{3})+|\d+")


def scrape_all_products(
    driver,
    page_limit: Optional[int] = None,
    on_page: Optional[Callable[[List[Dict[str, Any]], int], None]] = None,
    known_skus: Optional[set[str]] = None,
) -> List[Dict[str, Any]]:
    wait = WebDriverWait(driver, 25)
    products: List[Dict[str, Any]] = []
    page = 1

    while True:
        _wait_for_table_ready(driver, wait)
        rows = driver.find_elements(By.CSS_SELECTOR, ROWS_SELECTOR)
        if not rows:
            break

        print(f"[Scraper] P?gina {page}: {len(rows)} produtos vis?veis.")

        page_products: List[Dict[str, Any]] = []
        for index in range(len(rows)):
            summary, trigger = _extract_listing_summary(driver, index)
            listing_sku = summary.get("listing_sku")
            known = bool(known_skus) and listing_sku and listing_sku in known_skus
            result: Dict[str, Any]
            try:
                _open_modal(driver, trigger, wait)
                details = extract_modal_data(
                    driver, summary["product_id"], wait_timeout=25, light=known
                )
                result = {**summary, **details}
                if known:
                    result["scrape_error"] = None
            except Exception as exc:
                summary["scrape_error"] = str(exc)
                result = summary
            result["_existing_sku"] = bool(known)
            page_products.append(result)
            _throttle(SCRAPER_ROW_DELAY)

        if on_page:
            on_page(page_products, page)

        products.extend(page_products)

        if page_limit and page >= page_limit:
            print(f"[Scraper] Limite de {page_limit} p?ginas atingido, interrompendo scraping.")
            break

        _throttle(SCRAPER_PAGE_DELAY)
        if not _go_to_next_page(driver, wait):
            break
        page += 1

    return products


def _wait_for_table_ready(driver, wait: WebDriverWait) -> None:
    wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, ROWS_SELECTOR)))
    wait.until(_table_spinner_hidden)


def _table_spinner_hidden(driver) -> bool:
    spinners = driver.find_elements(By.CSS_SELECTOR, TABLE_SPINNER_SELECTOR)
    if not spinners:
        return True
    return all("hidden" in (spinner.get_attribute("class") or "") for spinner in spinners)


def _extract_listing_summary(driver, row_index: int) -> Tuple[Dict[str, Any], Any]:
    try:
        rows = driver.find_elements(By.CSS_SELECTOR, ROWS_SELECTOR)
        row = rows[row_index]
    except StaleElementReferenceException:
        rows = driver.find_elements(By.CSS_SELECTOR, ROWS_SELECTOR)
        row = rows[row_index]

    trigger = row.find_element(By.CSS_SELECTOR, MODAL_TRIGGER_SELECTOR)
    product_id_raw = trigger.get_attribute("data-id")
    if not product_id_raw:
        raise RuntimeError("Botão do modal não possui data-id.")
    product_id = int(product_id_raw)

    sku = _safe_text(row, "td:nth-child(1)")

    thumbnail_anchor = row.find_elements(By.CSS_SELECTOR, "td:nth-child(2) a")
    thumbnail_href = thumbnail_anchor[0].get_attribute("href") if thumbnail_anchor else None
    thumbnail_img = row.find_elements(By.CSS_SELECTOR, "td:nth-child(2) img")
    thumbnail_src = thumbnail_img[0].get_attribute("src") if thumbnail_img else None

    title_cell = row.find_element(By.CSS_SELECTOR, "td:nth-child(3)")
    listing_color = _first_text(title_cell.find_elements(By.CSS_SELECTOR, ".small"))

    title_lines = [line.strip() for line in title_cell.text.splitlines() if line.strip()]
    if listing_color and title_lines and title_lines[0].lower() == listing_color.lower():
        title_lines = title_lines[1:]
    listing_name = title_lines[0] if title_lines else ""

    listing_badges = _collect_badges(title_cell)

    model = _safe_text(row, "td:nth-child(4)")
    brand = _safe_text(row, "td:nth-child(5)")
    price_text = _safe_text(row, "td:nth-child(6)")

    stock_badge = row.find_elements(By.CSS_SELECTOR, "td:nth-child(7) span")
    stock_label = stock_badge[0].text.strip() if stock_badge else ""
    stock_tooltip = stock_badge[0].get_attribute("data-original-title") if stock_badge else ""
    stock_qty = _parse_quantity(stock_tooltip) or _parse_quantity(stock_label)

    summary = {
        "product_id": product_id,
        "listing_sku": sku or None,
        "listing_thumbnail": thumbnail_src,
        "listing_thumbnail_full": thumbnail_href,
        "listing_name": listing_name,
        "listing_color": listing_color,
        "listing_model": model or None,
        "listing_brand": brand or None,
        "listing_price_text": price_text or None,
        "listing_stock_badge": stock_label or None,
        "listing_stock_tooltip": stock_tooltip or "",
        "listing_available_qty": stock_qty,
        "listing_badges": listing_badges,
    }

    return summary, trigger


def _collect_badges(context) -> List[Dict[str, Any]]:
    badges = []
    for badge in context.find_elements(By.CSS_SELECTOR, "span.badge"):
        label = badge.text.strip()
        tooltip = badge.get_attribute("data-original-title") or ""
        badges.append({"label": label or None, "tooltip": tooltip or None})
    return badges


def _first_text(elements) -> str:
    for element in elements:
        text = element.text.strip()
        if text:
            return text
    return None


def _safe_text(context, selector: str) -> str:
    try:
        return context.find_element(By.CSS_SELECTOR, selector).text.strip()
    except NoSuchElementException:
        return ""


def _parse_quantity(raw: str, limit: int = QTY_MAX):
    """Extracts the first integer-looking chunk and caps to 32-bit to avoid overflow."""
    if not raw:
        return None
    cleaned = raw.replace("\xa0", " ").strip()
    if not cleaned:
        return None
    match = _QUANTITY_PATTERN.search(cleaned)
    if not match:
        return None
    digits_only = re.sub(r"\D", "", match.group())
    if not digits_only:
        return None
    value = int(digits_only)
    return min(value, limit)


def _open_modal(driver, trigger, wait: WebDriverWait) -> None:
    driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", trigger)
    wait.until(lambda d: trigger.is_displayed() and trigger.is_enabled())
    try:
        trigger.click()
    except ElementClickInterceptedException:
        driver.execute_script("arguments[0].click();", trigger)
    wait.until(EC.visibility_of_element_located((By.ID, "viewProduct")))


def _go_to_next_page(driver, wait: WebDriverWait) -> bool:
    next_container = driver.find_element(By.ID, "DataTables_Table_0_next")
    classes = next_container.get_attribute("class") or ""
    if "disabled" in classes:
        return False
    link = next_container.find_element(By.TAG_NAME, "a")
    driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", next_container)
    link.click()
    _wait_for_table_ready(driver, wait)
    return True


def _throttle(delay_seconds: float) -> None:
    """
    Applied between interactions to avoid racing against DOM updates.
    """
    if delay_seconds and delay_seconds > 0:
        time.sleep(delay_seconds)
