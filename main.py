import os

from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.common.by import By

from scraper.browser import start_browser
from scraper.list_scraper import scrape_all_products
from db.postgres import save_products
from config.settings import DATABASE_TARGETS, LOGIN_EMAIL, LOGIN_PASSWORD

LOGIN_URL = "https://app.obaobamix.com.br/login"
PRODUCTS_URL = "https://app.obaobamix.com.br/admin/products"

DEFAULT_TEST_PAGE_LIMIT = 5
PAGE_LIMIT_ENV = os.getenv("SCRAPER_PAGE_LIMIT")

try:
    PAGE_LIMIT = int(PAGE_LIMIT_ENV) if PAGE_LIMIT_ENV else DEFAULT_TEST_PAGE_LIMIT
except ValueError:
    PAGE_LIMIT = DEFAULT_TEST_PAGE_LIMIT

if PAGE_LIMIT and PAGE_LIMIT <= 0:
    PAGE_LIMIT = None


def prompt_manual_login(driver) -> None:
    driver.get(LOGIN_URL)
    _prefill_login_form(driver)
    input(
        "[Login] Revisamos o formulário com suas credenciais. Resolva o CAPTCHA/manual login e pressione ENTER quando concluir..."
    )


def _prefill_login_form(driver) -> None:
    """Preenche o formulário de login com as credenciais do .env (quando disponíveis)."""
    if not LOGIN_EMAIL and not LOGIN_PASSWORD:
        return

    def fill(selector: str, value: str | None):
        if not value:
            return
        try:
            element = driver.find_element(By.CSS_SELECTOR, selector)
            element.clear()
            element.send_keys(value)
        except NoSuchElementException:
            pass

    fill("input[name='email'], input#email, input[type='email']", LOGIN_EMAIL)
    fill("input[name='password'], input#password, input[type='password']", LOGIN_PASSWORD)


def main():
    driver = start_browser()
    try:
        prompt_manual_login(driver)
        driver.get(PRODUCTS_URL)

        products = scrape_all_products(driver, page_limit=PAGE_LIMIT)
        print(f"[Scraper] {len(products)} produtos coletados.")

        if products:
            for label, db_config in DATABASE_TARGETS:
                try:
                    persisted = save_products(products, db_config)
                    print(f"[Scraper][{label}] {persisted} registros gravados.")
                except Exception as exc:
                    print(f"[Scraper][{label}] Falha ao gravar: {exc}")
                    if label == "local":
                        raise
        else:
            print("[Scraper] Nenhum produto para gravar.")
    finally:
        driver.quit()


if __name__ == "__main__":
    main()
