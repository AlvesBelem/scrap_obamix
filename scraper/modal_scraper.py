import re
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, List, Optional

from selenium.common.exceptions import TimeoutException
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

MODAL_ID = "viewProduct"
INT32_MAX = 2_147_483_647
QTY_MAX = 1_000_000
TAB_CONTENT_TIMEOUT = 15
_QUANTITY_PATTERN = re.compile(r"\d{1,3}(?:[.\s]\d{3})+|\d+")


def extract_modal_data(driver, product_id: int, wait_timeout: int = 25, light: bool = False) -> Dict[str, Any]:
    """
    Captura todos os campos visíveis no modal do produto, incluindo texto,
    listas, flags e links de imagem.
    """
    wait = WebDriverWait(driver, wait_timeout)
    try:
        wait.until(EC.visibility_of_element_located((By.ID, MODAL_ID)))
        wait.until(_modal_content_ready)
    except TimeoutException as exc:
        _close_modal(driver)
        raise RuntimeError("Modal não carregou completamente antes do timeout.") from exc

    modal = driver.find_element(By.ID, MODAL_ID)
    data: Dict[str, Any] = {"product_id": product_id}

    data["sku"] = _text_or_none(modal, "#modal-sku")
    data["name"] = _clean_modal_name(_text_or_none(modal, "#modal-name"))

    price_text = _text_or_none(modal, "#modal-price")
    data["price_text"] = price_text
    data["price_brl"] = _parse_decimal(price_text)

    stock_badge = modal.find_elements(By.CSS_SELECTOR, "#modal-inv span.badge")
    if stock_badge:
        badge = stock_badge[0]
        tooltip = badge.get_attribute("data-original-title") or ""
        label = badge.text.strip()
        data["stock_label"] = label or None
        data["stock_tooltip"] = tooltip
        data["available_qty"] = _parse_quantity(tooltip) or _parse_quantity(label)
    else:
        data["stock_label"] = None
        data["stock_tooltip"] = ""
        data["available_qty"] = None

    price_min_alert = modal.find_elements(By.ID, "price-min-alert")
    price_min_value = None
    if price_min_alert:
        is_hidden = "hidden" in (price_min_alert[0].get_attribute("class") or "")
        if not is_hidden:
            price_min_value = _parse_decimal(_text_or_none(modal, "#price-min"))
    data["price_min_brl"] = price_min_value

    if light:
        return data

    data["brand"] = _text_or_none(modal, "#modal-brand")
    data["model"] = _text_or_none(modal, "#modal-model")
    data["color"] = _text_or_none(modal, "#modal-color")
    data["voltage"] = _text_or_none(modal, "#modal-voltage")
    data["ean"] = _text_or_none(modal, "#modal-ean")
    data["ncm"] = _text_or_none(modal, "#modal-ncm")
    data["anatel"] = _text_or_none(modal, "#modal-anatel")
    data["inmetro"] = _text_or_none(modal, "#modal-inmetro")

    weight_text = _text_or_none(modal, "#modal-weight")
    data["weight_kg"] = _parse_decimal(weight_text)
    data["dimensions_cm"] = _text_or_none(modal, "#modal-size")

    data["categories"] = _collect_badge_values(modal, "#modal-categories span.badge")
    data["flags"] = _collect_labeled_badges(modal, "#modal-flags span")

    description_element = modal.find_elements(By.ID, "modal-description")
    data["description_html"] = (
        description_element[0].get_attribute("innerHTML").strip() if description_element else None
    )

    notices_element = modal.find_elements(By.ID, "modal-notices")
    data["notices_html"] = (
        notices_element[0].get_attribute("innerHTML").strip() if notices_element else None
    )

    data["top_keywords"] = _collect_list_items(modal, "#modal-top-keys")
    data["title_suggestions"] = _collect_list_items(modal, "#modal-top-titles")

    video_iframe = modal.find_elements(By.CSS_SELECTOR, "#nav-video iframe")
    data["video_url"] = video_iframe[0].get_attribute("src") if video_iframe else None

    main_image = modal.find_elements(By.ID, "modal-image")
    main_image_src = main_image[0].get_attribute("src") if main_image else None
    main_image_link = modal.find_elements(By.ID, "modal-href-image")
    main_image_href = main_image_link[0].get_attribute("href") if main_image_link else None

    data["main_image"] = main_image_src
    data["main_image_full"] = main_image_href

    data["images"] = _collect_gallery(modal, main_image_src, main_image_href)

    try:
        return data
    finally:
        _close_modal(driver)


def _modal_content_ready(driver) -> bool:
    """Verifica se o spinner do modal desapareceu e o título foi preenchido."""
    modal = driver.find_element(By.ID, MODAL_ID)
    title = modal.find_element(By.ID, "modal-name").text.strip()
    spinner = modal.find_elements(By.CSS_SELECTOR, ".loadingModal")
    spinner_hidden = True
    if spinner:
        spinner_hidden = "hidden" in (spinner[0].get_attribute("class") or "")
    return bool(title) and spinner_hidden


def _text_or_none(context, selector: str) -> Optional[str]:
    elements = context.find_elements(By.CSS_SELECTOR, selector)
    if not elements:
        return None
    text = elements[0].text.strip()
    return text or None


def _clean_modal_name(text: Optional[str]) -> Optional[str]:
    if not text:
        return None
    cleaned = text.replace("Clique para copiar", "").strip()
    return cleaned or None


def _parse_decimal(value: Optional[str]) -> Optional[Decimal]:
    if not value:
        return None
    normalized = (
        value.replace("R$", "")
        .replace("r$", "")
        .replace("\u00a0", "")
        .replace(".", "")
        .replace(" ", "")
        .replace(",", ".")
    )
    try:
        return Decimal(normalized)
    except InvalidOperation:
        return None


def _parse_quantity(raw: Optional[str], limit: int = QTY_MAX) -> Optional[int]:
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


def _collect_badge_values(context, selector: str) -> List[str]:
    values = []
    for badge in context.find_elements(By.CSS_SELECTOR, selector):
        text = badge.text.strip()
        if text:
            values.append(text)
    return values


def _collect_labeled_badges(context, selector: str) -> List[Dict[str, Optional[str]]]:
    badges: List[Dict[str, Optional[str]]] = []
    for badge in context.find_elements(By.CSS_SELECTOR, selector):
        label = badge.text.strip()
        tooltip = badge.get_attribute("data-original-title") or badge.get_attribute("title") or ""
        badges.append({"label": label or None, "tooltip": tooltip or None})
    return badges


def _collect_list_items(modal, container_selector: str) -> List[str]:
    container = _ensure_tab_content_loaded(modal, container_selector, wait_timeout=TAB_CONTENT_TIMEOUT)
    if container is None:
        return []

    li_elements = container.find_elements(By.TAG_NAME, "li")
    items: List[str] = []
    for li in li_elements:
        text = li.text.strip()
        if text:
            items.append(text)

    if items:
        return items

    raw_text = container.text.strip()
    if not raw_text:
        return []

    lines = [line.strip() for line in raw_text.splitlines() if line.strip()]
    if lines and lines[0].endswith(":"):
        lines = lines[1:]

    return lines


def _ensure_tab_content_loaded(modal, container_selector: str, wait_timeout: int = 10):
    container = _refind(modal, container_selector)
    if container is None:
        return None

    if _container_has_content(container):
        return container

    driver = modal.parent
    tab_container = None
    try:
        tab_container = container.find_element(By.XPATH, "./ancestor::div[contains(@class,'tab-pane')]")
    except Exception:
        pass

    if tab_container is not None:
        tab_id = tab_container.get_attribute("id")
        trigger_selectors = [
            f'[data-bs-target="#{tab_id}"]',
            f'[data-target="#{tab_id}"]',
            f'a[href="#{tab_id}"]',
        ]

        trigger = None
        for selector in trigger_selectors:
            matches = modal.find_elements(By.CSS_SELECTOR, selector)
            if matches:
                trigger = matches[0]
                break

        if trigger:
            driver.execute_script("arguments[0].click();", trigger)

    wait = WebDriverWait(driver, wait_timeout)
    try:
        wait.until(lambda _: _container_has_content(_refind(modal, container_selector)))
    except TimeoutException:
        pass

    return _refind(modal, container_selector)


def _container_has_content(element) -> bool:
    if element is None:
        return False
    if element.find_elements(By.TAG_NAME, "li"):
        return True
    text = element.text.strip()
    return bool(text)


def _refind(modal, selector: str):
    elements = modal.find_elements(By.CSS_SELECTOR, selector)
    return elements[0] if elements else None


def _collect_gallery(modal, main_src: Optional[str], main_href: Optional[str]) -> List[Dict[str, Any]]:
    gallery: List[Dict[str, Any]] = []

    if main_src:
        gallery.append(
            {
                "url": main_src,
                "href": main_href or main_src,
                "is_main": True,
                "position": 0,
            }
        )

    thumbnails = modal.find_elements(By.CSS_SELECTOR, "#modal-media img")
    for idx, img in enumerate(thumbnails, start=1):
        url = img.get_attribute("src")
        if not url:
            continue
        try:
            href = img.find_element(By.XPATH, "./ancestor::a[1]").get_attribute("href")
        except Exception:
            href = url
        if any(entry["url"] == url for entry in gallery):
            continue
        gallery.append({"url": url, "href": href or url, "is_main": False, "position": idx})

    return gallery


def _close_modal(driver) -> None:
    try:
        close_button = driver.find_element(By.CSS_SELECTOR, f"#{MODAL_ID} button.close")
        close_button.click()
    except Exception:
        try:
            driver.find_element(By.TAG_NAME, "body").send_keys(Keys.ESCAPE)
        except Exception:
            pass

    try:
        WebDriverWait(driver, 10).until(EC.invisibility_of_element_located((By.ID, MODAL_ID)))
    except TimeoutException:
        pass
