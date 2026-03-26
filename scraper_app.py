"""
Morimens Wiki Image Scraper
- Selenium-based with tkinter GUI
- Uses default Chrome profile (visible browser)
- Translates Chinese filenames/folders to English
- Downloads original full-size images
"""

import base64
import os
import re
import signal
import sys
import time
import subprocess
import threading
import traceback
import tkinter as tk
from tkinter import ttk, scrolledtext
from urllib.parse import unquote, urlparse

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (
    TimeoutException, NoSuchElementException,
    StaleElementReferenceException, WebDriverException,
)
from deep_translator import GoogleTranslator


# ═══════════════════════════════════════════════════════════════
#  CONSTANTS
# ═══════════════════════════════════════════════════════════════

BASE_URL = "https://morimens.huijiwiki.com"

EXCLUDED_WIKI_PREFIXES = [
    "Special:", "Help:", "Talk:", "Template:", "Category:",
    "File:", "User:", "MediaWiki:", "Module:",
]

TASKS = [
    # (id, display_name, method_name)
    ("characters",       "1.  Characters (唤醒体)",            "scrape_characters"),
    ("monsters",         "2.  Monsters (怪物)",                "scrape_monsters"),
    ("card_stack",       "3.  Card Stack (牌堆栈)",            "scrape_card_stack"),
    ("fate_wheel",       "4.  Fate Wheel (命轮)",              "scrape_fate_wheel"),
    ("secret_contract",  "5.  Secret Contract (密契)",         "scrape_secret_contract"),
    ("key_orders",       "6.  Key Orders (钥令)",              "scrape_key_orders"),
    ("materials",        "7.  Materials (材料)",                "scrape_materials"),
    ("creations",        "8.  Creations (造物)",               "scrape_creations"),
    ("engravings",       "9.  Engravings (刻印)",              "scrape_engravings"),
    ("events",           "10. Events (事件)",                  "scrape_events"),
    ("achievements",     "11. Achievements (成就)",            "scrape_achievements"),
    ("investigation",    "12. Investigation (调查行动)",       "scrape_investigation"),
    ("activities",       "13. Activities (活动)",              "scrape_activities"),
    ("awakening",        "14. Awakening (唤醒)",               "scrape_awakening"),
    ("awakening_sim",    "15. Awakening Sim (唤醒模拟)",       "scrape_awakening_sim"),
    ("avatars",          "16. Keeper Avatars (头像)",          "scrape_avatars"),
    ("avatar_frames",    "17. Avatar Frames (头像框)",         "scrape_avatar_frames"),
    ("cg_wallpapers",    "18. CG Wallpapers (壁纸)",          "scrape_cg_wallpapers"),
    ("gallery",          "19. Gallery (画廊)",                 "scrape_gallery"),
    ("voice_actors",     "20. Voice Actors (声优)",            "scrape_voice_actors"),
    ("diluvian",         "21. Diluvian Chronicles (洪积纪事)", "scrape_diluvian"),
    ("cite_collection",  "22. Cité Collection (西岱收藏)",     "scrape_cite_collection"),
]


# ═══════════════════════════════════════════════════════════════
#  UTILITIES
# ═══════════════════════════════════════════════════════════════

def sanitize_filename(name: str) -> str:
    name = re.sub(r'[<>:"/\\|?*\x00-\x1f]', "_", name)
    name = name.strip(". ")
    return name if name else "unnamed"


def get_original_image_url(src: str) -> str | None:
    """Convert a huiji thumbnail URL to the original full-size URL."""
    if not src:
        return None
    if "/thumb/" not in src:
        return src
    url = src.replace(
        "huiji-thumb.huijistatic.com", "huiji-public.huijistatic.com"
    )
    url = url.replace("/uploads/thumb/", "/uploads/")
    last_slash = url.rfind("/")
    if last_slash > 0:
        url = url[:last_slash]
    return url


def get_ext_from_url(url: str) -> str:
    path = unquote(urlparse(url).path)
    basename = path.split("/")[-1]
    basename = re.sub(r"^\d+px-", "", basename)
    _, ext = os.path.splitext(basename)
    return ext.lower() if ext else ".png"


def is_content_link(href: str, current_url: str | None = None) -> bool:
    if not href:
        return False
    if current_url and href.split("#")[0] == current_url.split("#")[0]:
        return False
    for prefix in EXCLUDED_WIKI_PREFIXES:
        if f"/wiki/{prefix}" in href:
            return False
    return "/wiki/" in href


# ═══════════════════════════════════════════════════════════════
#  TRANSLATION CACHE
# ═══════════════════════════════════════════════════════════════

class TranslationCache:
    def __init__(self):
        self._cache: dict[str, str] = {}
        self._translator = GoogleTranslator(source="zh-CN", target="en")

    def translate(self, text: str) -> str:
        if not text or not text.strip():
            return text
        text = text.strip()
        if text in self._cache:
            return self._cache[text]
        # Already English?
        if re.match(r"^[a-zA-Z0-9\s_\-\.☆★]+$", text):
            self._cache[text] = text
            return text
        for attempt in range(3):
            try:
                result = self._translator.translate(text)
                if result and result.strip():
                    self._cache[text] = result.strip()
                    return result.strip()
            except Exception:
                time.sleep(0.5 * (attempt + 1))
        # Fallback to original
        self._cache[text] = text
        return text


# ═══════════════════════════════════════════════════════════════
#  SCRAPER ENGINE
# ═══════════════════════════════════════════════════════════════

class ImageScraper:
    def __init__(self, log_fn, stop_event: threading.Event):
        self.log = log_fn
        self.stop_event = stop_event
        self.driver = None
        self.trans = TranslationCache()
        self.base_dir = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "scraped_images"
        )
        self._downloaded_urls: set[str] = set()

    # ── helpers ────────────────────────────────────────────────
    @property
    def stopped(self):
        return self.stop_event.is_set()

    def _ensure(self, path):
        os.makedirs(path, exist_ok=True)
        return path

    def _translate_filename(self, chinese_name, fallback_ext=".png"):
        name, ext = os.path.splitext(chinese_name)
        if not ext:
            ext = fallback_ext
        if not name:
            return "unnamed" + ext
        translated = self.trans.translate(name)
        safe = sanitize_filename(translated)
        return (safe if safe else "unnamed") + ext

    def _translate_folder(self, chinese_name):
        translated = self.trans.translate(chinese_name)
        return sanitize_filename(translated) or "unnamed"

    def _img_name(self, img):
        alt = None
        try:
            alt = img.get_attribute("alt")
        except StaleElementReferenceException:
            pass
        if alt and alt.strip():
            return alt.strip()
        try:
            src = img.get_attribute("src") or ""
        except StaleElementReferenceException:
            return "unnamed.png"
        path = unquote(urlparse(src).path)
        fname = path.split("/")[-1]
        return re.sub(r"^\d+px-", "", fname)

    # ── browser control ────────────────────────────────────────
    def init_driver(self):
        if self.driver:
            return
        self.log("Starting Chrome...")
        opts = Options()
        # Use a dedicated profile so we don't fight with an open Chrome
        selenium_profile = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "chrome_selenium_profile"
        )
        opts.add_argument(f"--user-data-dir={selenium_profile}")
        opts.add_argument("--disable-blink-features=AutomationControlled")
        opts.add_argument("--no-first-run")
        opts.add_argument("--no-default-browser-check")
        opts.add_experimental_option("excludeSwitches", ["enable-automation"])
        opts.add_experimental_option("useAutomationExtension", False)
        self.driver = webdriver.Chrome(options=opts)

        # Give Chrome time to fully initialize before interacting
        time.sleep(3)

        # Try to set anti-detection, but don't fail if Chrome isn't ready yet
        for attempt in range(5):
            try:
                self.driver.execute_script(
                    "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
                )
                break
            except WebDriverException:
                time.sleep(2)

        try:
            self.driver.maximize_window()
        except WebDriverException:
            try:
                time.sleep(2)
                self.driver.maximize_window()
            except WebDriverException:
                self.log("Could not maximize window, continuing...")

        self.log("Chrome started successfully.")

    def close_driver(self):
        if self.driver:
            try:
                self.driver.quit()
            except Exception:
                pass
            self.driver = None

    def navigate(self, url):
        self.driver.get(url)
        try:
            WebDriverWait(self.driver, 20).until(
                lambda d: d.execute_script("return document.readyState") == "complete"
            )
        except TimeoutException:
            pass
        time.sleep(1.5)

    def scroll_page(self, max_scrolls=20):
        last_h = self.driver.execute_script("return document.body.scrollHeight")
        for _ in range(max_scrolls):
            self.driver.execute_script(
                "window.scrollTo(0, document.body.scrollHeight);"
            )
            time.sleep(0.7)
            new_h = self.driver.execute_script("return document.body.scrollHeight")
            if new_h == last_h:
                break
            last_h = new_h
        self.driver.execute_script("window.scrollTo(0, 0);")
        time.sleep(0.4)

    # ── downloading ────────────────────────────────────────────
    def _js_fetch(self, url):
        """Download an image via the browser's own fetch() to bypass anti-hotlinking."""
        script = """
        async function dl(url) {
            try {
                const resp = await fetch(url, {credentials: 'include'});
                if (!resp.ok) return null;
                const blob = await resp.blob();
                return await new Promise((resolve, reject) => {
                    const reader = new FileReader();
                    reader.onloadend = () => resolve(reader.result);
                    reader.onerror = reject;
                    reader.readAsDataURL(blob);
                });
            } catch(e) { return null; }
        }
        return await dl(arguments[0]);
        """
        try:
            data_url = self.driver.execute_script(script, url)
            if data_url and "," in data_url:
                return base64.b64decode(data_url.split(",", 1)[1])
        except WebDriverException:
            pass
        return None

    def download_image(self, url, folder, chinese_name=None, fallback_url=None):
        if self.stopped or not url:
            return False
        if url in self._downloaded_urls:
            return True

        ext = get_ext_from_url(url)
        if chinese_name:
            filename = self._translate_filename(chinese_name, ext)
        else:
            raw = unquote(urlparse(url).path).split("/")[-1]
            raw = re.sub(r"^\d+px-", "", raw)
            filename = self._translate_filename(raw, ext)

        self._ensure(folder)
        filepath = os.path.join(folder, filename)

        # Handle name collisions from different images
        if os.path.exists(filepath):
            if url in self._downloaded_urls:
                return True
            base, fext = os.path.splitext(filepath)
            counter = 2
            while os.path.exists(f"{base}_{counter}{fext}"):
                counter += 1
            filepath = f"{base}_{counter}{fext}"

        try:
            data = self._js_fetch(url)
            if not data and fallback_url and fallback_url != url:
                data = self._js_fetch(fallback_url)
            if not data:
                self.log(f"  ✗ {filename} — download failed")
                return False
            with open(filepath, "wb") as f:
                f.write(data)
            self._downloaded_urls.add(url)
            self.log(f"  ✓ {os.path.basename(filepath)}")
            return True
        except Exception as exc:
            self.log(f"  ✗ {filename} — {exc}")
            return False

    def collect_images(self, folder):
        """Download every huiji-hosted image on the current page."""
        self.scroll_page()
        imgs = self.driver.find_elements(By.TAG_NAME, "img")
        page_seen: set[str] = set()
        count = 0
        for img in imgs:
            if self.stopped:
                break
            try:
                src = img.get_attribute("src") or ""
            except StaleElementReferenceException:
                continue
            if "huijistatic.com" not in src or "/uploads/" not in src:
                continue
            orig = get_original_image_url(src)
            if not orig or orig in page_seen:
                continue
            page_seen.add(orig)
            cname = self._img_name(img)
            self.download_image(orig, folder, cname, fallback_url=src)
            count += 1
        self.log(f"  Page total: {count} images.")
        return count

    # ── link discovery ─────────────────────────────────────────
    def _content_links(self, selector=None, cur_url=None):
        sel = selector or ".mw-parser-output a[href*='/wiki/']"
        cur = cur_url or self.driver.current_url
        elems = self.driver.find_elements(By.CSS_SELECTOR, sel)
        links, seen = [], set()
        for el in elems:
            try:
                href = el.get_attribute("href")
                title = el.get_attribute("title") or el.text or ""
            except StaleElementReferenceException:
                continue
            if not href or not is_content_link(href, cur):
                continue
            base = href.split("#")[0]
            if base in seen:
                continue
            seen.add(base)
            links.append((href, title.strip()))
        return links

    def _img_links(self, cur_url=None):
        cur = cur_url or self.driver.current_url
        imgs = self.driver.find_elements(
            By.CSS_SELECTOR,
            ".mw-parser-output img[src*='huijistatic.com']",
        )
        links, seen = [], set()
        for img in imgs:
            try:
                parent_a = img.find_element(By.XPATH, "./ancestor::a[1]")
                href = parent_a.get_attribute("href")
                alt = img.get_attribute("alt") or ""
            except (NoSuchElementException, StaleElementReferenceException):
                continue
            if not href or not is_content_link(href, cur):
                continue
            base = href.split("#")[0]
            if base in seen:
                continue
            seen.add(base)
            name = re.sub(r"\.(png|jpg|jpeg|webp|gif)$", "", alt, flags=re.I).strip()
            links.append((href, name))
        return links

    # ═══════════════════════════════════════════════════════════
    #  SCRAPING METHODS — one per wiki page
    # ═══════════════════════════════════════════════════════════

    def _simple(self, url, folder_name, title):
        folder = os.path.join(self.base_dir, folder_name)
        self.log(f"=== {title} ===")
        self.navigate(url)
        self.collect_images(folder)

    # ── 1. Characters (唤醒体) ─────────────────────────────────
    def scrape_characters(self):
        url = "https://morimens.huijiwiki.com/wiki/%E5%94%A4%E9%86%92%E4%BD%93"
        folder = os.path.join(self.base_dir, "Characters")
        self.log("=== 1. Characters (唤醒体) ===")
        self.navigate(url)
        self.scroll_page()

        char_links = self._img_links(url)
        if not char_links:
            char_links = self._content_links(cur_url=url)
        self.log(f"Found {len(char_links)} characters.")

        for i, (href, name) in enumerate(char_links):
            if self.stopped:
                break
            fname = self._translate_folder(name) if name else f"character_{i+1}"
            char_folder = os.path.join(folder, fname)
            self.log(f"[{i+1}/{len(char_links)}] {name} → {fname}")
            self.navigate(href)
            self.collect_images(char_folder)

    # ── 2. Monsters (怪物) ─────────────────────────────────────
    def scrape_monsters(self):
        self._simple(
            "https://morimens.huijiwiki.com/wiki/%E6%80%AA%E7%89%A9",
            "Monsters", "2. Monsters (怪物)",
        )

    # ── 3. Card Stack (牌堆栈) ─────────────────────────────────
    def scrape_card_stack(self):
        self._simple(
            "https://morimens.huijiwiki.com/wiki/%E7%89%8C%E5%A0%86%E6%A0%88",
            "Card_Stack", "3. Card Stack (牌堆栈)",
        )

    # ── 4. Fate Wheel / Weapons (命轮) ─────────────────────────
    def scrape_fate_wheel(self):
        url = "https://morimens.huijiwiki.com/wiki/%E5%91%BD%E8%BD%AE"
        folder = os.path.join(self.base_dir, "Fate_Wheel")
        self.log("=== 4. Fate Wheel / Weapons (命轮) ===")
        self.navigate(url)
        self.scroll_page()

        # Selector targets only links inside table cells (weapon entries)
        td_selector = "td a[href*='/wiki/']"

        # Pagination – look for page buttons
        page_btns = self.driver.find_elements(
            By.CSS_SELECTOR,
            ".pagination-0 .topage, #pagination .topage, "
            ".mw-parser-output .topage, .tabber .topage",
        )
        n_pages = max(len(page_btns), 1)
        self.log(f"Found {n_pages} pagination page(s).")

        all_links: list[tuple[str, str]] = []
        seen_bases: set[str] = set()

        for p in range(n_pages):
            if self.stopped:
                break
            if p > 0:
                try:
                    btns = self.driver.find_elements(
                        By.CSS_SELECTOR,
                        ".pagination-0 .topage, #pagination .topage, "
                        ".mw-parser-output .topage, .tabber .topage",
                    )
                    if p < len(btns):
                        self.driver.execute_script(
                            "arguments[0].scrollIntoView(true);", btns[p]
                        )
                        time.sleep(0.3)
                        btns[p].click()
                        time.sleep(2)
                        self.scroll_page()
                except Exception as exc:
                    self.log(f"  Pagination error p{p+1}: {exc}")
                    continue

            # Collect only td-cell links (weapon entries)
            page_links = self._content_links(selector=td_selector, cur_url=url)
            for href, title in page_links:
                base = href.split("#")[0]
                if base not in seen_bases and title:
                    seen_bases.add(base)
                    all_links.append((href, title))
            self.log(f"  Page {p+1}: +{len(page_links)} links  (total unique: {len(all_links)})")

        self.log(f"Visiting {len(all_links)} weapon pages for single image each...")
        self._ensure(folder)
        for i, (href, name) in enumerate(all_links):
            if self.stopped:
                break
            self.log(f"[{i+1}/{len(all_links)}] {name}")
            self.navigate(href)

            # Grab only the single weapon image inside div.floatnone
            try:
                img = self.driver.find_element(
                    By.CSS_SELECTOR, "div.floatnone img[src*='huijistatic.com']"
                )
                src = img.get_attribute("src") or ""
                alt = img.get_attribute("alt") or name or ""
                orig = get_original_image_url(src) if src else None
                if orig:
                    cname = re.sub(r"\.(png|jpg|jpeg|webp|gif)$", "", alt, flags=re.I).strip()
                    self.download_image(orig, folder, cname or name, fallback_url=src)
                else:
                    self.log(f"  No valid image src found")
            except NoSuchElementException:
                self.log(f"  No div.floatnone image found on this page")

    # ── 5. Secret Contract (密契) ──────────────────────────────
    def scrape_secret_contract(self):
        url = "https://morimens.huijiwiki.com/wiki/%E5%AF%86%E5%A5%91"
        folder = os.path.join(self.base_dir, "Secret_Contract")
        self.log("=== 5. Secret Contract (密契) ===")
        self.navigate(url)
        self.scroll_page()

        # Main page images
        self.log("Collecting main page images...")
        self.collect_images(folder)

        # Sub-links (span-wrapped links)
        sub = self._content_links(
            ".mw-parser-output span a[href*='/wiki/']", url
        )
        if not sub:
            sub = self._content_links(cur_url=url)
        self.log(f"Found {len(sub)} sub-sections.")

        for i, (href, name) in enumerate(sub):
            if self.stopped:
                break
            fname = self._translate_folder(name) if name else f"section_{i+1}"
            sf = os.path.join(folder, fname)
            self.log(f"[{i+1}/{len(sub)}] {name} → {fname}")
            self.navigate(href)
            self.collect_images(sf)

    # ── 6. Key Orders (钥令) ───────────────────────────────────
    def scrape_key_orders(self):
        url = "https://morimens.huijiwiki.com/wiki/%E9%92%A5%E4%BB%A4"
        folder = os.path.join(self.base_dir, "Key_Orders")
        self.log("=== 6. Key Orders (钥令) ===")
        self.navigate(url)
        self.scroll_page()

        self.log("Collecting main page images...")
        self.collect_images(folder)

        item_links = self._img_links(url)
        self.log(f"Found {len(item_links)} item links.")
        for i, (href, name) in enumerate(item_links):
            if self.stopped:
                break
            self.log(f"[{i+1}/{len(item_links)}] {name}")
            self.navigate(href)
            self.collect_images(folder)

    # ── 7. Materials (材料) ────────────────────────────────────
    def scrape_materials(self):
        self._visit_links_one_folder(
            "https://morimens.huijiwiki.com/wiki/%E6%9D%90%E6%96%99",
            "Materials", "7. Materials (材料)",
        )

    # ── 8. Creations (造物) ────────────────────────────────────
    def scrape_creations(self):
        self._visit_links_one_folder(
            "https://morimens.huijiwiki.com/wiki/%E9%80%A0%E7%89%A9",
            "Creations", "8. Creations (造物)",
        )

    # ── 9. Engravings (刻印) ───────────────────────────────────
    def scrape_engravings(self):
        self._simple(
            "https://morimens.huijiwiki.com/wiki/%E5%88%BB%E5%8D%B0",
            "Engravings", "9. Engravings (刻印)",
        )

    # ── 10. Events (事件) ──────────────────────────────────────
    def scrape_events(self):
        url = "https://morimens.huijiwiki.com/wiki/%E4%BA%8B%E4%BB%B6"
        folder = os.path.join(self.base_dir, "Events")
        self.log("=== 10. Events (事件) ===")
        self.navigate(url)
        self.scroll_page()

        links = self._img_links(url)
        if not links:
            links = self._content_links(cur_url=url)
        self.log(f"Found {len(links)} event links.")

        for i, (href, name) in enumerate(links):
            if self.stopped:
                break
            self.log(f"[{i+1}/{len(links)}] {name}")
            self.navigate(href)
            self.collect_images(folder)

    # ── 11. Achievements (成就) ────────────────────────────────
    def scrape_achievements(self):
        self._simple(
            "https://morimens.huijiwiki.com/wiki/%E6%88%90%E5%B0%B1",
            "Achievements", "11. Achievements (成就)",
        )

    # ── 12. Investigation (调查行动) ──────────────────────────
    def scrape_investigation(self):
        self._simple(
            "https://morimens.huijiwiki.com/wiki/%E8%B0%83%E6%9F%A5%E8%A1%8C%E5%8A%A8",
            "Investigation", "12. Investigation (调查行动)",
        )

    # ── 13. Activities (活动) ──────────────────────────────────
    def scrape_activities(self):
        url = "https://morimens.huijiwiki.com/wiki/%E6%B4%BB%E5%8A%A8"
        folder = os.path.join(self.base_dir, "Activities")
        self.log("=== 13. Activities (活动) ===")
        self.navigate(url)
        self.scroll_page()

        links = self._img_links(url)
        if not links:
            links = self._content_links(cur_url=url)
        self.log(f"Found {len(links)} activity links.")

        for i, (href, name) in enumerate(links):
            if self.stopped:
                break
            self.log(f"[{i+1}/{len(links)}] {name}")
            self.navigate(href)
            self.collect_images(folder)

    # ── 14. Awakening (唤醒) ───────────────────────────────────
    def scrape_awakening(self):
        self._simple(
            "https://morimens.huijiwiki.com/wiki/%E5%94%A4%E9%86%92",
            "Awakening", "14. Awakening (唤醒)",
        )

    # ── 15. Awakening Simulation (唤醒模拟) ────────────────────
    def scrape_awakening_sim(self):
        self._simple(
            "https://morimens.huijiwiki.com/wiki/%E5%94%A4%E9%86%92%E6%A8%A1%E6%8B%9F",
            "Awakening_Simulation", "15. Awakening Simulation (唤醒模拟)",
        )

    # ── 16. Keeper Avatars (守密人头像) ────────────────────────
    def scrape_avatars(self):
        self._simple(
            "https://morimens.huijiwiki.com/wiki/%E5%AE%88%E5%AF%86%E4%BA%BA%E5%A4%B4%E5%83%8F",
            "Keeper_Avatars", "16. Keeper Avatars (守密人头像)",
        )

    # ── 17. Avatar Frames (守密人头像框) ──────────────────────
    def scrape_avatar_frames(self):
        self._simple(
            "https://morimens.huijiwiki.com/wiki/%E5%AE%88%E5%AF%86%E4%BA%BA%E5%A4%B4%E5%83%8F%E6%A1%86",
            "Keeper_Avatar_Frames", "17. Avatar Frames (守密人头像框)",
        )

    # ── 18. CG Wallpapers (CG壁纸) ────────────────────────────
    def scrape_cg_wallpapers(self):
        self._simple(
            "https://morimens.huijiwiki.com/wiki/CG%E5%A3%81%E7%BA%B8",
            "CG_Wallpapers", "18. CG Wallpapers (CG壁纸)",
        )

    # ── 19. Gallery (画廊) ─────────────────────────────────────
    def scrape_gallery(self):
        self._simple(
            "https://morimens.huijiwiki.com/wiki/%E7%94%BB%E5%BB%8A",
            "Gallery", "19. Gallery (画廊)",
        )

    # ── 20. Voice Actors (声优) ────────────────────────────────
    def scrape_voice_actors(self):
        url = "https://morimens.huijiwiki.com/wiki/%E5%A3%B0%E4%BC%98"
        folder = os.path.join(self.base_dir, "Voice_Actors")
        self.log("=== 20. Voice Actors (声优) ===")
        self.navigate(url)
        self.scroll_page()

        char_links = self._img_links(url)
        self.log(f"Found {len(char_links)} voice-actor entries.")

        for i, (href, name) in enumerate(char_links):
            if self.stopped:
                break
            fname = self._translate_folder(name) if name else f"va_{i+1}"
            sf = os.path.join(folder, fname)
            self.log(f"[{i+1}/{len(char_links)}] {name} → {fname}")
            self.navigate(href)
            self.collect_images(sf)

    # ── 21. Diluvian Chronicles (洪积纪事本末) ─────────────────
    def scrape_diluvian(self):
        self._simple(
            "https://morimens.huijiwiki.com/wiki/%E6%B4%AA%E7%A7%AF%E7%BA%AA%E4%BA%8B%E6%9C%AC%E6%9C%AB",
            "Diluvian_Chronicles", "21. Diluvian Chronicles (洪积纪事本末)",
        )

    # ── 22. Cité Collection (西岱收藏馆) ──────────────────────
    def scrape_cite_collection(self):
        self._simple(
            "https://morimens.huijiwiki.com/wiki/%E8%A5%BF%E5%B2%B1%E6%94%B6%E8%97%8F%E9%A6%86",
            "Cite_Collection", "22. Cité Collection (西岱收藏馆)",
        )

    # ── shared helper: visit links → one folder ───────────────
    def _visit_links_one_folder(self, url, folder_name, title):
        folder = os.path.join(self.base_dir, folder_name)
        self.log(f"=== {title} ===")
        self.navigate(url)
        self.scroll_page()

        links = self._content_links(cur_url=url)
        self.log(f"Found {len(links)} links to visit.")

        for i, (href, name) in enumerate(links):
            if self.stopped:
                break
            self.log(f"[{i+1}/{len(links)}] {name}")
            self.navigate(href)
            self.collect_images(folder)


# ═══════════════════════════════════════════════════════════════
#  GUI
# ═══════════════════════════════════════════════════════════════

class ScraperApp:
    def __init__(self):
        self.root = tk.Tk()
        self.root.title("Morimens Wiki Image Scraper")
        self.root.geometry("1000x780")
        self.root.minsize(750, 520)
        self.root.configure(bg="#2b2b2b")

        self.stop_event = threading.Event()
        self.scraper: ImageScraper | None = None
        self.thread: threading.Thread | None = None

        self._build_ui()
        self.root.protocol("WM_DELETE_WINDOW", self.close)

    # ── UI construction ────────────────────────────────────────
    def _build_ui(self):
        style = ttk.Style()
        style.theme_use("clam")
        style.configure("TFrame", background="#2b2b2b")
        style.configure("TLabel", background="#2b2b2b", foreground="#cccccc")
        style.configure("TLabelframe", background="#2b2b2b", foreground="#cccccc")
        style.configure("TLabelframe.Label", background="#2b2b2b", foreground="#aaaaaa")
        style.configure(
            "TButton",
            padding=5,
            font=("Segoe UI", 9),
            background="#3c3c3c",
            foreground="#dddddd",
        )
        style.configure(
            "Accent.TButton",
            padding=6,
            font=("Segoe UI", 10, "bold"),
            background="#0078d4",
            foreground="#ffffff",
        )

        # ─ top bar ─
        top = ttk.Frame(self.root, padding=6)
        top.pack(fill=tk.X)

        ttk.Button(
            top, text="▶  SCRAPE ALL", style="Accent.TButton", command=self.start_all
        ).pack(side=tk.LEFT, padx=3)
        ttk.Button(top, text="⏹  Stop", command=self.stop).pack(side=tk.LEFT, padx=3)
        ttk.Button(top, text="📋  Copy Log", command=self.copy_log).pack(
            side=tk.LEFT, padx=3
        )
        ttk.Button(top, text="✕  Close", command=self.close).pack(
            side=tk.LEFT, padx=3
        )

        self.status = tk.StringVar(value="Ready — close Chrome before starting")
        ttk.Label(top, textvariable=self.status, font=("Segoe UI", 9)).pack(
            side=tk.RIGHT, padx=8
        )

        # ─ task buttons (scrollable) ─
        task_frame = ttk.LabelFrame(self.root, text="Individual Tasks", padding=4)
        task_frame.pack(fill=tk.X, padx=8, pady=4)

        canvas = tk.Canvas(
            task_frame, height=210, bg="#2b2b2b", highlightthickness=0
        )
        vsb = ttk.Scrollbar(task_frame, orient=tk.VERTICAL, command=canvas.yview)
        inner = ttk.Frame(canvas)
        inner.bind(
            "<Configure>",
            lambda e: canvas.configure(scrollregion=canvas.bbox("all")),
        )
        canvas.create_window((0, 0), window=inner, anchor="nw")
        canvas.configure(yscrollcommand=vsb.set)
        canvas.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        vsb.pack(side=tk.RIGHT, fill=tk.Y)
        canvas.bind_all(
            "<MouseWheel>",
            lambda e: canvas.yview_scroll(-1 * (e.delta // 120), "units"),
        )

        cols = 3
        for i, (_tid, tname, method) in enumerate(TASKS):
            r, c = divmod(i, cols)
            ttk.Button(
                inner,
                text=tname,
                command=lambda m=method, n=tname: self.start_single(m, n),
            ).grid(row=r, column=c, padx=3, pady=2, sticky="ew")
        for c in range(cols):
            inner.columnconfigure(c, weight=1)

        # ─ log area ─
        log_frame = ttk.LabelFrame(self.root, text="Log", padding=4)
        log_frame.pack(fill=tk.BOTH, expand=True, padx=8, pady=4)

        self.log_widget = scrolledtext.ScrolledText(
            log_frame,
            wrap=tk.WORD,
            font=("Consolas", 9),
            bg="#1e1e1e",
            fg="#d4d4d4",
            insertbackground="white",
        )
        self.log_widget.pack(fill=tk.BOTH, expand=True)

    # ── logging ────────────────────────────────────────────────
    def log(self, msg):
        ts = time.strftime("%H:%M:%S")

        def _append():
            self.log_widget.insert(tk.END, f"[{ts}] {msg}\n")
            self.log_widget.see(tk.END)

        self.root.after(0, _append)

    def copy_log(self):
        self.root.clipboard_clear()
        self.root.clipboard_append(self.log_widget.get("1.0", tk.END))
        self.log("Log copied to clipboard.")

    # ── control ────────────────────────────────────────────────
    def stop(self):
        self.stop_event.set()
        self.status.set("Stopping…")
        self.log("Stop requested.")

    def close(self):
        self.stop_event.set()
        if self.scraper:
            self.scraper.close_driver()
        self.root.destroy()

    def _busy(self):
        if self.thread and self.thread.is_alive():
            self.log("A task is already running — stop it first.")
            return True
        return False

    def _run(self, methods, label):
        self.stop_event.clear()
        self.root.after(0, lambda: self.status.set(f"Running: {label}"))
        try:
            scraper = ImageScraper(self.log, self.stop_event)
            self.scraper = scraper
            scraper.init_driver()
            for m in methods:
                if self.stop_event.is_set():
                    break
                getattr(scraper, m)()
            if self.stop_event.is_set():
                self.log("Stopped by user.")
            else:
                self.log("═══ ALL DONE ═══")
        except Exception as exc:
            self.log(f"ERROR: {exc}")
            self.log(traceback.format_exc())
        finally:
            if self.scraper:
                self.scraper.close_driver()
                self.scraper = None
            self.root.after(0, lambda: self.status.set("Ready"))

    def start_single(self, method, name):
        if self._busy():
            return
        self.thread = threading.Thread(
            target=self._run, args=([method], name), daemon=True
        )
        self.thread.start()

    def start_all(self):
        if self._busy():
            return
        all_m = [m for _, _, m in TASKS]
        self.thread = threading.Thread(
            target=self._run, args=(all_m, "All Tasks"), daemon=True
        )
        self.thread.start()

    def run(self):
        self.root.mainloop()


# ═══════════════════════════════════════════════════════════════
#  ENTRY POINT
# ═══════════════════════════════════════════════════════════════

if __name__ == "__main__":
    # Ignore Ctrl+C in the terminal — use the GUI Stop/Close buttons
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    app = ScraperApp()
    app.run()
