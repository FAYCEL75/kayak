import time
import random
import traceback
import re
from urllib.parse import quote_plus

import undetected_chromedriver as uc
from selenium.webdriver.common.by import By


USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 12_6) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124 Safari/537.36",
]
GLOBAL_DRIVER = None


def extract_score(raw):
    if not raw:
        return None
    m = re.search(r"(\d+[.,]?\d*)", raw)
    if not m:
        return None
    try:
        return float(m.group(1).replace(",", "."))
    except:
        return None


def build_url(city: str):
    return f"https://www.booking.com/searchresults.fr.html?ss={quote_plus(city)}"


# -------------------------------------------------------------
# DRIVER UC (INDETECTABLE)
# -------------------------------------------------------------
def _get_driver():
    global GLOBAL_DRIVER

    if GLOBAL_DRIVER is not None:
        return GLOBAL_DRIVER

    opts = uc.ChromeOptions()
    opts.add_argument("--no-first-run")
    opts.add_argument("--no-service-autorun")
    opts.add_argument("--no-default-browser-check")
    opts.add_argument("--disable-popup-blocking")
    opts.add_argument("--disable-notifications")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--no-sandbox")
    opts.add_argument(f"--user-agent={random.choice(USER_AGENTS)}")

    # un SEUL Chrome
    GLOBAL_DRIVER = uc.Chrome(options=opts, headless=False)
    GLOBAL_DRIVER.set_page_load_timeout(30)
    return GLOBAL_DRIVER


# -------------------------------------------------------------
# SCRAPER COMPATIBLE AVEC TON ETL (city, max_hotels, retries)
# -------------------------------------------------------------
def scrape_booking(city, max_hotels=20, retries=3):

    url = build_url(city)

    for attempt in range(1, retries + 1):
        print(f"Scraping Booking --> {city} (tentative {attempt}/{retries})")

        try:
            driver = _get_driver()
            driver.get(url)
            time.sleep(3)

            # 🔥 ESSENTIEL : accepter les cookies si présent
            try:
                btn = driver.find_element(By.CSS_SELECTOR, "button[aria-label='Accepter']")
                btn.click()
                time.sleep(1)
            except:
                pass

            # 🔥 SIMULER UN HUMAIN : scroll progressif + pause + mouvement souris
            last_height = 0
            for _ in range(10):  # 10 scrolls => charge ~40 hôtels
                driver.execute_script("window.scrollBy(0, 1200);")
                time.sleep(random.uniform(0.6, 1.2))

                # petit mouvement de souris : casse les anti-bots
                try:
                    driver.execute_script(
                        "document.querySelector('body').dispatchEvent(new MouseEvent('mousemove', "
                        "{clientX:100, clientY:200}))"
                    )
                except:
                    pass

                new_height = driver.execute_script("return document.body.scrollHeight;")
                if new_height == last_height:
                    break
                last_height = new_height

            # 🔥 Maintenant on récupère tous les cards
            cards = driver.find_elements(By.CSS_SELECTOR, "[data-testid='property-card']")
            hotels = []

            for card in cards[:max_hotels]:

                # name
                try:
                    name = card.find_element(By.CSS_SELECTOR, "[data-testid='title']").text.strip()
                except:
                    continue

                # score
                try:
                    score_raw = card.find_element(By.CSS_SELECTOR, "[data-testid='review-score']").text.strip()
                    score = extract_score(score_raw)
                except:
                    score = None

                if not name or score is None:
                    continue

                # price
                price = None
                try:
                    price_raw = card.find_element(By.CSS_SELECTOR, "[data-testid='price-and-discounted-price']").text
                    digits = "".join(c for c in price_raw if c.isdigit())
                    price = int(digits) if digits else None
                except:
                    pass

                # url
                try:
                    url_hotel = card.find_element(By.TAG_NAME, "a").get_attribute("href")
                except:
                    url_hotel = None

                hotels.append({
                    "city": city,
                    "hotelName": name,
                    "score": score,
                    "price_eur": price,
                    "url": url_hotel
                })

            # NE PAS FERMER LE NAVIGATEUR
            # driver.quit()  ❌ on enlève
            pass


            print(f"➡️ Hotels trouvés = {len(hotels)}")
            if len(hotels) >= 10:  # on accepte à partir de 10
                return hotels

            print("⚠️ Pas assez d'hôtels, retry...")
            time.sleep(2)

        except Exception as e:
            print(f"[ERR] {city}: {e}")
            traceback.print_exc()
            time.sleep(1)

    print(f"[FATAL] Échec scraping Booking : {city}")
    return []
