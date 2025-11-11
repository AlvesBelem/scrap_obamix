from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd
import psycopg2
from psycopg2 import OperationalError, errorcodes, sql
from psycopg2.extras import execute_batch

BASE_DIR = Path(__file__).resolve().parents[1]
EXPORT_XLSX_PATH = BASE_DIR / "produtos_export.xlsx"

PRODUCTS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS products (
    product_id INTEGER PRIMARY KEY,
    sku TEXT,
    name TEXT,
    price_brl NUMERIC(12, 2),
    price_venda NUMERIC(12, 2),
    price_min_brl NUMERIC(12, 2),
    brand TEXT,
    model TEXT,
    color TEXT,
    voltage TEXT,
    ean TEXT,
    ncm TEXT,
    anatel TEXT,
    inmetro TEXT,
    weight_kg NUMERIC(10, 3),
    dimensions_cm TEXT,
    description_html TEXT,
    notices_html TEXT,
    stock_label TEXT,
    stock_tooltip TEXT,
    available_qty INTEGER,
    listing_sku TEXT,
    listing_name TEXT,
    listing_color TEXT,
    listing_brand TEXT,
    listing_model TEXT,
    listing_price_text TEXT,
    listing_stock_badge TEXT,
    listing_stock_tooltip TEXT,
    listing_available_qty INTEGER,
    listing_thumbnail TEXT,
    listing_thumbnail_full TEXT,
    main_image TEXT,
    main_image_full TEXT,
    video_url TEXT,
    scrape_error TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);
"""

DETAIL_TABLES_SQL = [
    """
    CREATE TABLE IF NOT EXISTS product_categories (
        id BIGSERIAL PRIMARY KEY,
        product_id INTEGER REFERENCES products(product_id) ON DELETE CASCADE,
        category TEXT NOT NULL
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS product_flags (
        id BIGSERIAL PRIMARY KEY,
        product_id INTEGER REFERENCES products(product_id) ON DELETE CASCADE,
        label TEXT,
        tooltip TEXT
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS product_images (
        product_id INTEGER REFERENCES products(product_id) ON DELETE CASCADE,
        url TEXT NOT NULL,
        href TEXT,
        is_main BOOLEAN DEFAULT FALSE,
        position INTEGER,
        PRIMARY KEY (product_id, url)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS product_keywords (
        id BIGSERIAL PRIMARY KEY,
        product_id INTEGER REFERENCES products(product_id) ON DELETE CASCADE,
        keyword TEXT NOT NULL
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS product_titles (
        id BIGSERIAL PRIMARY KEY,
        product_id INTEGER REFERENCES products(product_id) ON DELETE CASCADE,
        title TEXT NOT NULL
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS product_listing_badges (
        id BIGSERIAL PRIMARY KEY,
        product_id INTEGER REFERENCES products(product_id) ON DELETE CASCADE,
        label TEXT,
        tooltip TEXT
    );
    """,
]

UPSERT_PRODUCTS_SQL = """
INSERT INTO products (
    product_id, sku, name, price_brl, price_venda, price_min_brl, brand, model, color,
    voltage, ean, ncm, anatel, inmetro, weight_kg, dimensions_cm, description_html,
    notices_html, stock_label, stock_tooltip, available_qty, listing_sku,
    listing_name, listing_color, listing_brand, listing_model, listing_price_text,
    listing_stock_badge, listing_stock_tooltip, listing_available_qty,
    listing_thumbnail, listing_thumbnail_full, main_image, main_image_full,
    video_url, scrape_error
) VALUES (
    %(product_id)s, %(sku)s, %(name)s, %(price_brl)s, %(price_venda)s, %(price_min_brl)s, %(brand)s,
    %(model)s, %(color)s, %(voltage)s, %(ean)s, %(ncm)s, %(anatel)s, %(inmetro)s,
    %(weight_kg)s, %(dimensions_cm)s, %(description_html)s, %(notices_html)s,
    %(stock_label)s, %(stock_tooltip)s, %(available_qty)s, %(listing_sku)s,
    %(listing_name)s, %(listing_color)s, %(listing_brand)s, %(listing_model)s,
    %(listing_price_text)s, %(listing_stock_badge)s, %(listing_stock_tooltip)s,
    %(listing_available_qty)s, %(listing_thumbnail)s, %(listing_thumbnail_full)s,
    %(main_image)s, %(main_image_full)s, %(video_url)s, %(scrape_error)s
)
ON CONFLICT (product_id) DO UPDATE SET
    sku = EXCLUDED.sku,
    name = EXCLUDED.name,
    price_brl = EXCLUDED.price_brl,
    price_venda = EXCLUDED.price_venda,
    price_min_brl = EXCLUDED.price_min_brl,
    brand = EXCLUDED.brand,
    model = EXCLUDED.model,
    color = EXCLUDED.color,
    voltage = EXCLUDED.voltage,
    ean = EXCLUDED.ean,
    ncm = EXCLUDED.ncm,
    anatel = EXCLUDED.anatel,
    inmetro = EXCLUDED.inmetro,
    weight_kg = EXCLUDED.weight_kg,
    dimensions_cm = EXCLUDED.dimensions_cm,
    description_html = EXCLUDED.description_html,
    notices_html = EXCLUDED.notices_html,
    stock_label = EXCLUDED.stock_label,
    stock_tooltip = EXCLUDED.stock_tooltip,
    available_qty = EXCLUDED.available_qty,
    listing_sku = EXCLUDED.listing_sku,
    listing_name = EXCLUDED.listing_name,
    listing_color = EXCLUDED.listing_color,
    listing_brand = EXCLUDED.listing_brand,
    listing_model = EXCLUDED.listing_model,
    listing_price_text = EXCLUDED.listing_price_text,
    listing_stock_badge = EXCLUDED.listing_stock_badge,
    listing_stock_tooltip = EXCLUDED.listing_stock_tooltip,
    listing_available_qty = EXCLUDED.listing_available_qty,
    listing_thumbnail = EXCLUDED.listing_thumbnail,
    listing_thumbnail_full = EXCLUDED.listing_thumbnail_full,
    main_image = EXCLUDED.main_image,
    main_image_full = EXCLUDED.main_image_full,
    video_url = EXCLUDED.video_url,
    scrape_error = EXCLUDED.scrape_error,
    updated_at = NOW();
"""

INSERT_CATEGORIES_SQL = """
INSERT INTO product_categories (product_id, category)
VALUES (%(product_id)s, %(category)s);
"""

INSERT_FLAGS_SQL = """
INSERT INTO product_flags (product_id, label, tooltip)
VALUES (%(product_id)s, %(label)s, %(tooltip)s);
"""

INSERT_IMAGES_SQL = """
INSERT INTO product_images (product_id, url, href, is_main, position)
VALUES (%(product_id)s, %(url)s, %(href)s, %(is_main)s, %(position)s)
ON CONFLICT (product_id, url) DO UPDATE SET
    href = EXCLUDED.href,
    is_main = EXCLUDED.is_main,
    position = EXCLUDED.position;
"""

INSERT_KEYWORDS_SQL = """
INSERT INTO product_keywords (product_id, keyword)
VALUES (%(product_id)s, %(keyword)s);
"""

INSERT_TITLES_SQL = """
INSERT INTO product_titles (product_id, title)
VALUES (%(product_id)s, %(title)s);
"""

INSERT_LISTING_BADGES_SQL = """
INSERT INTO product_listing_badges (product_id, label, tooltip)
VALUES (%(product_id)s, %(label)s, %(tooltip)s);
"""


def connect_db(config: Dict[str, Any], attempt_create: bool = True):
    """
    Abre conexão com o banco alvo. Se o banco não existir, cria automaticamente
    utilizando o database de manutenção (postgres por padrão).
    """
    try:
        return psycopg2.connect(
            dbname=config["dbname"],
            user=config["user"],
            password=config["password"],
            host=config["host"],
            port=config["port"],
        )
    except OperationalError as exc:
        pgcode = getattr(exc, "pgcode", None)
        message = str(exc).lower()
        db_missing = (
            pgcode == errorcodes.INVALID_CATALOG_NAME
            or "does not exist" in message
            or "não existe" in message
        )
        if not attempt_create or not db_missing:
            raise

        _create_database(config)
        return connect_db(config, attempt_create=False)


def _create_database(config: Dict[str, Any]) -> None:
    maintenance_db = config.get("maintenance_db", "postgres")
    connection = psycopg2.connect(
        dbname=maintenance_db,
        user=config["user"],
        password=config["password"],
        host=config["host"],
        port=config["port"],
    )
    connection.autocommit = True

    try:
        with connection.cursor() as cur:
            dbname = sql.Identifier(config["dbname"])
            try:
                cur.execute(sql.SQL("CREATE DATABASE {}").format(dbname))
                print(f"[DB] Database '{config['dbname']}' criado automaticamente.")
            except psycopg2.errors.DuplicateDatabase:
                print(f"[DB] Database '{config['dbname']}' já existe.")
    finally:
        connection.close()


def save_products(products: List[Dict[str, Any]], config: Dict[str, Any]) -> int:
    if not products:
        return 0

    frames = _build_frames(products)
    _export_to_excel(frames)
    product_ids = frames["products"]["product_id"].tolist()

    conn = connect_db(config)
    try:
        with conn:
            with conn.cursor() as cur:
                _ensure_tables(cur)
                execute_batch(cur, UPSERT_PRODUCTS_SQL, frames["products"].to_dict("records"))

                _replace_detail(
                    cur,
                    "product_categories",
                    frames["categories"],
                    INSERT_CATEGORIES_SQL,
                    product_ids,
                )
                _replace_detail(
                    cur,
                    "product_flags",
                    frames["flags"],
                    INSERT_FLAGS_SQL,
                    product_ids,
                )
                _replace_detail(
                    cur,
                    "product_images",
                    frames["images"],
                    INSERT_IMAGES_SQL,
                    product_ids,
                )
                _replace_detail(
                    cur,
                    "product_keywords",
                    frames["keywords"],
                    INSERT_KEYWORDS_SQL,
                    product_ids,
                )
                _replace_detail(
                    cur,
                    "product_titles",
                    frames["titles"],
                    INSERT_TITLES_SQL,
                    product_ids,
                )
                _replace_detail(
                    cur,
                    "product_listing_badges",
                    frames["listing_badges"],
                    INSERT_LISTING_BADGES_SQL,
                    product_ids,
                )
        return len(products)
    finally:
        conn.close()


def _ensure_tables(cur) -> None:
    cur.execute(PRODUCTS_TABLE_SQL)
    cur.execute("ALTER TABLE products ADD COLUMN IF NOT EXISTS price_venda NUMERIC(12, 2);")
    for ddl in DETAIL_TABLES_SQL:
        cur.execute(ddl)


def _replace_detail(cur, table_name: str, frame: pd.DataFrame, insert_sql: str, product_ids: List[int]) -> None:
    if not product_ids:
        return
    cur.execute(f"DELETE FROM {table_name} WHERE product_id = ANY(%s)", (product_ids,))
    if frame.empty:
        return
    execute_batch(cur, insert_sql, frame.to_dict("records"))


def _build_frames(products: List[Dict[str, Any]]) -> Dict[str, pd.DataFrame]:
    df_products = pd.DataFrame(
        [
            {
                "product_id": p["product_id"],
                "sku": p.get("sku") or p.get("listing_sku"),
                "name": p.get("name") or p.get("listing_name"),
                "price_brl": p.get("price_brl"),
                "price_venda": None,  # placeholder for insertion order
                "price_min_brl": p.get("price_min_brl"),
                "brand": p.get("brand") or p.get("listing_brand"),
                "model": p.get("model") or p.get("listing_model"),
                "color": p.get("color") or p.get("listing_color"),
                "voltage": p.get("voltage"),
                "ean": p.get("ean"),
                "ncm": p.get("ncm"),
                "anatel": p.get("anatel"),
                "inmetro": p.get("inmetro"),
                "weight_kg": p.get("weight_kg"),
                "dimensions_cm": p.get("dimensions_cm"),
                "description_html": p.get("description_html"),
                "notices_html": p.get("notices_html"),
                "stock_label": p.get("stock_label") or p.get("listing_stock_badge"),
                "stock_tooltip": p.get("stock_tooltip") or p.get("listing_stock_tooltip"),
                "available_qty": p.get("available_qty") or p.get("listing_available_qty"),
                "listing_sku": p.get("listing_sku"),
                "listing_name": p.get("listing_name"),
                "listing_color": p.get("listing_color"),
                "listing_brand": p.get("listing_brand"),
                "listing_model": p.get("listing_model"),
                "listing_price_text": p.get("listing_price_text"),
                "listing_stock_badge": p.get("listing_stock_badge"),
                "listing_stock_tooltip": p.get("listing_stock_tooltip"),
                "listing_available_qty": p.get("listing_available_qty"),
                "listing_thumbnail": p.get("listing_thumbnail"),
                "listing_thumbnail_full": p.get("listing_thumbnail_full"),
                "main_image": p.get("main_image"),
                "main_image_full": p.get("main_image_full"),
                "video_url": p.get("video_url"),
                "scrape_error": p.get("scrape_error"),
            }
            for p in products
        ]
    )

    df_products = _inject_price_venda(df_products)

    df_categories = _build_simple_frame(products, "categories", "category")
    df_flags = _build_dict_frame(products, "flags", ["label", "tooltip"])
    df_images = _build_dict_frame(products, "images", ["url", "href", "is_main", "position"])
    df_keywords = _build_simple_frame(products, "top_keywords", "keyword")
    df_titles = _build_simple_frame(products, "title_suggestions", "title")
    df_listing_badges = _build_dict_frame(products, "listing_badges", ["label", "tooltip"])

    return {
        "products": df_products,
        "categories": df_categories,
        "flags": df_flags,
        "images": df_images,
        "keywords": df_keywords,
        "titles": df_titles,
        "listing_badges": df_listing_badges,
    }


def _build_simple_frame(products: List[Dict[str, Any]], key: str, column_name: str) -> pd.DataFrame:
    rows = []
    for product in products:
        values = product.get(key) or []
        for value in values:
            rows.append({"product_id": product["product_id"], column_name: value})
    return pd.DataFrame(rows)


def _build_dict_frame(products: List[Dict[str, Any]], key: str, columns: List[str]) -> pd.DataFrame:
    rows: List[Dict[str, Any]] = []
    for product in products:
        entries = product.get(key) or []
        for entry in entries:
            row = {"product_id": product["product_id"]}
            for column in columns:
                row[column] = entry.get(column)
            rows.append(row)
    return pd.DataFrame(rows)


def _inject_price_venda(df_products: pd.DataFrame) -> pd.DataFrame:
    if df_products.empty:
        df_products["price_venda"] = pd.Series(dtype=float)
        return df_products

    def calc_price_venda(value):
        if value is None:
            return None
        try:
            if pd.isna(value):
                return None
        except TypeError:
            pass

        try:
            decimal_value = Decimal(str(value))
        except (InvalidOperation, TypeError, ValueError):
            return None
        return decimal_value * Decimal("2.56")

    df_products["price_venda"] = df_products["price_brl"].apply(calc_price_venda)
    columns = df_products.columns.tolist()
    price_idx = columns.index("price_brl")
    # move price_venda to immediately after price_brl
    columns.insert(price_idx + 1, columns.pop(columns.index("price_venda")))
    return df_products[columns]


def _export_to_excel(frames: Dict[str, pd.DataFrame]) -> None:
    EXPORT_XLSX_PATH.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(EXPORT_XLSX_PATH, engine="openpyxl") as writer:
        for name, frame in frames.items():
            sheet_name = name[:31]
            frame.to_excel(writer, sheet_name=sheet_name, index=False)
