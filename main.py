import os

from scraper.browser import start_browser
from scraper.list_scraper import scrape_all_products
from db.postgres import save_products
from config.settings import DB_CONFIG

LOGIN_URL = "https://app.obaobamix.com.br/login"
PRODUCTS_URL = "https://app.obaobamix.com.br/admin/products"

# Define um limite de páginas via variável de ambiente.
# Para o teste atual, configure SCRAPER_PAGE_LIMIT=2.
PAGE_LIMIT = os.getenv("SCRAPER_PAGE_LIMIT")
PAGE_LIMIT = int(PAGE_LIMIT) if PAGE_LIMIT and PAGE_LIMIT.isdigit() else None


def prompt_manual_login(driver) -> None:
    driver.get(LOGIN_URL)
    input("[Login] Resolva o CAPTCHA e faça login no navegador aberto. Pressione ENTER para continuar...")


def main():
    driver = start_browser()
    try:
        prompt_manual_login(driver)
        driver.get(PRODUCTS_URL)

        products = scrape_all_products(driver, page_limit=PAGE_LIMIT)
        print(f"[Scraper] {len(products)} produtos coletados.")

        if products:
            persisted = save_products(products, DB_CONFIG)
            print(f"[Scraper] {persisted} registros gravados no PostgreSQL.")
        else:
            print("[Scraper] Nenhum produto para gravar.")
    finally:
        driver.quit()


if __name__ == "__main__":
    main()
