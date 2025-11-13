from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.selenium_manager import SeleniumManager


def start_browser():
    options = webdriver.ChromeOptions()
    driver_path = _resolve_driver_path(options)
    service = Service(executable_path=driver_path)
    driver = webdriver.Chrome(service=service, options=options)
    driver.maximize_window()
    return driver


def _resolve_driver_path(options: webdriver.ChromeOptions) -> str:
    """Force Selenium Manager to ignore stale drivers found in PATH."""
    args = ["--browser", "chrome", "--skip-driver-in-path"]
    binary_location = getattr(options, "binary_location", None)
    if binary_location:
        args.extend(["--browser-path", binary_location])

    result = SeleniumManager().binary_paths(args)
    driver_path = result.get("driver_path")
    if not driver_path:
        raise RuntimeError("Selenium Manager nao retornou o caminho do ChromeDriver.")
    return driver_path
