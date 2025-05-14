#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Công cụ trích xuất URL YouTube từ Havamath - Phiên bản tối ưu với thông tin chương
------------------------------------------
Tác giả: Claude
Phiên bản: 2.1
Mô tả: Trích xuất URL YouTube và thông tin chương từ các bài giảng trên Havamath
"""

import json
import re
import os
import time
import argparse
import traceback
from datetime import datetime
from urllib.parse import urlparse
from concurrent.futures import ThreadPoolExecutor, as_completed

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

try:
    from webdriver_manager.chrome import ChromeDriverManager

    WEBDRIVER_MANAGER_AVAILABLE = True
except ImportError:
    WEBDRIVER_MANAGER_AVAILABLE = False

try:
    from bs4 import BeautifulSoup
    import requests

    BS4_AVAILABLE = True
except ImportError:
    BS4_AVAILABLE = False


class HavamathExtractor:
    def __init__(self, cookies_file=None, headless=True, verbose=True, wait_time=10, debug=False,
                 max_workers=4, simplified_output=True, reuse_driver=False):
        """Khởi tạo trình trích xuất"""
        self.cookies_file = cookies_file
        self.headless = headless
        self.verbose = verbose
        self.wait_time = wait_time
        self.debug = debug
        self.max_workers = max_workers
        self.simplified_output = simplified_output
        self.reuse_driver = reuse_driver

        self.driver = None
        self.worker_drivers = {}
        self.session = None
        self.chapters = []  # Danh sách các chương

        # Thiết lập requests session nếu có BeautifulSoup
        if BS4_AVAILABLE:
            self.session = requests.Session()
            self.session.headers.update({
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.5',
            })

            # Tải cookies cho session requests
            if cookies_file and os.path.exists(cookies_file):
                self._load_cookies_to_requests()

    def _log(self, message):
        """In thông báo nếu chế độ verbose được bật"""
        if self.verbose:
            print(message)

    def _debug_log(self, message):
        """In thông báo debug nếu chế độ debug được bật"""
        if self.debug:
            print(f"[DEBUG] {message}")

    def _init_driver(self, worker_id=None):
        """Khởi tạo trình duyệt Chrome"""
        # Nếu đang sử dụng lại driver và đã có driver chính
        if worker_id is None and self.reuse_driver and self.driver is not None:
            return self.driver

        # Nếu đang sử dụng lại driver cho worker và đã có driver cho worker này
        if worker_id is not None and self.reuse_driver and worker_id in self.worker_drivers:
            return self.worker_drivers[worker_id]

        chrome_options = Options()
        if self.headless:
            chrome_options.add_argument("--headless")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--window-size=1920,1080")
        chrome_options.add_argument("--disable-notifications")
        chrome_options.add_argument("--mute-audio")
        chrome_options.add_argument("--disable-logging")
        chrome_options.add_argument("--log-level=3")

        # Sử dụng webdriver-manager nếu có
        if WEBDRIVER_MANAGER_AVAILABLE:
            driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
        else:
            driver = webdriver.Chrome(options=chrome_options)

        # Tải cookies
        if self.cookies_file and os.path.exists(self.cookies_file):
            self._load_cookies_to_driver(driver)

        # Lưu driver
        if worker_id is None:
            self.driver = driver
        else:
            self.worker_drivers[worker_id] = driver

        return driver

    def _load_cookies_to_requests(self):
        """Tải cookies vào requests session"""
        try:
            with open(self.cookies_file, 'r') as f:
                cookies = json.load(f)

                # Chuyển đổi cookies sang định dạng requests
                for cookie in cookies:
                    self.session.cookies.set(
                        cookie['name'],
                        cookie['value'],
                        domain=cookie.get('domain', ''),
                        path=cookie.get('path', '/')
                    )
            return True
        except Exception as e:
            self._log(f"Lỗi khi tải cookies cho requests: {e}")
            return False

    def _clean_cookies(self, cookies):
        """Dọn dẹp cookies để tránh lỗi khi thêm vào Selenium"""
        cleaned_cookies = []
        for cookie in cookies:
            # Tạo bản sao để tránh thay đổi cookie gốc
            clean_cookie = cookie.copy()

            # Loại bỏ trường không cần thiết
            if 'storeId' in clean_cookie:
                del clean_cookie['storeId']

            # Sửa trường sameSite
            if 'sameSite' in clean_cookie and clean_cookie['sameSite'] is None:
                clean_cookie['sameSite'] = 'Lax'

            cleaned_cookies.append(clean_cookie)

        return cleaned_cookies

    def _load_cookies_to_driver(self, driver):
        """Tải cookies vào Selenium WebDriver"""
        # Truy cập domain trước
        domain = 'havamath.vn'
        driver.get(f"https://{domain}")
        time.sleep(2)  # Cho phép trang tải

        try:
            with open(self.cookies_file, 'r') as f:
                cookies = json.load(f)

                # Dọn dẹp cookies
                cleaned_cookies = self._clean_cookies(cookies)

                for cookie in cleaned_cookies:
                    # Chuyển đổi expiry thành int nếu là float
                    if 'expiry' in cookie:
                        cookie['expiry'] = int(cookie['expiry'])

                    try:
                        driver.add_cookie(cookie)
                    except Exception as e:
                        if self.debug:
                            self._debug_log(f"Lỗi khi thêm cookie {cookie.get('name')}: {e}")
                        continue

            # Làm mới trang để áp dụng cookies
            driver.refresh()
            time.sleep(1)
            return True

        except Exception as e:
            self._log(f"Lỗi khi tải cookies cho driver: {e}")
            return False

    def extract_course_id(self, course_url):
        """Trích xuất ID khóa học từ URL"""
        parsed_url = urlparse(course_url)
        path_parts = parsed_url.path.strip('/').split('/')
        if len(path_parts) >= 2 and path_parts[0] == 'courses':
            return path_parts[1]
        return None

    def get_iso_time(self):
        """Trả về thời gian hiện tại theo định dạng ISO 8601"""
        return datetime.now().strftime("%Y-%m-%dT%H:%M:%S.000Z")

    def _extract_chapters(self, course_url):
        """Trích xuất thông tin các chương từ trang khóa học"""
        self._log("Đang trích xuất thông tin chương...")

        driver = self._init_driver()
        driver.get(course_url)
        time.sleep(5)  # Đợi trang tải đầy đủ

        # Tìm các phần tử có thể là chương
        chapters = []

        try:
            # Phương pháp 1: Tìm các thẻ heading (h1, h2, h3, h4, h5) hoặc div có class chứa chapter, section, module
            possible_chapter_elements = driver.find_elements(By.CSS_SELECTOR,
                                                             "h1, h2, h3, h4, h5, div.chapter, div.section, div.module, div[class*='chapter'], " +
                                                             "div[class*='section'], div[class*='module'], div[class*='course-section']")

            current_chapter = {"title": "Chương không xác định", "lectures": []}

            for elem in possible_chapter_elements:
                text = elem.text.strip()
                tag_name = elem.tag_name
                classes = elem.get_attribute("class") or ""

                # Kiểm tra xem đây có phải là tiêu đề chương không
                is_chapter_title = (
                        tag_name in ['h1', 'h2', 'h3', 'h4', 'h5'] or
                        'chapter' in classes.lower() or
                        'section' in classes.lower() or
                        'module' in classes.lower() or
                        bool(re.search(r'chương|phần|module|section|chapter', text.lower()))
                )

                if is_chapter_title and text and len(text) < 200:  # Một tiêu đề chương thường ngắn
                    # Thêm chương hiện tại vào danh sách (nếu có) và tạo chương mới
                    if current_chapter["lectures"]:
                        chapters.append(current_chapter)

                    current_chapter = {"title": text, "lectures": []}

            # Thêm chương cuối cùng vào danh sách nếu có
            if current_chapter["lectures"] or not chapters:
                chapters.append(current_chapter)

            # Nếu không tìm thấy chương nào, tạo một chương mặc định
            if not chapters:
                chapters = [{"title": "Toàn khóa học", "lectures": []}]

            # Tìm các bài giảng và phân bổ vào chương
            all_lectures = []
            lecture_elements = driver.find_elements(By.CSS_SELECTOR, "a[href*='/learn/']")

            chapter_idx = 0
            for elem in lecture_elements:
                href = elem.get_attribute('href')
                if href and '/learn/' in href:
                    title = elem.text.strip()
                    if not title:
                        title_elem = elem.find_elements(By.TAG_NAME, 'span')
                        if title_elem:
                            title = title_elem[0].text.strip()

                    # Bỏ qua các bài giảng "Vào học"
                    if title and title != "Vào học":
                        # Kiểm tra xem bài giảng này thuộc về chương nào
                        # Phương pháp đơn giản: gán bài giảng cho chương hiện tại
                        chapter_idx = min(chapter_idx, len(chapters) - 1)
                        chapters[chapter_idx]["lectures"].append({
                            "title": title,
                            "url": href
                        })
                        all_lectures.append({
                            "title": title,
                            "url": href,
                            "chapter": chapters[chapter_idx]["title"]
                        })

                        # Nếu title giống tiêu đề chương, chuyển sang chương tiếp theo
                        if chapter_idx < len(chapters) - 1 and title == chapters[chapter_idx + 1]["title"]:
                            chapter_idx += 1

            # Phương pháp 2: Sử dụng JavaScript để phân tích cấu trúc DOM phức tạp hơn
            if not all_lectures:
                try:
                    chapter_data = driver.execute_script("""
                        // Hàm để tìm các phần tử có thể là chương
                        function findChapters() {
                            // Tìm các phần tử có thể là container của chương
                            const possibleContainers = Array.from(document.querySelectorAll(
                                'div.course-content, div.curriculum, div.syllabus, div[class*="chapter"], ' +
                                'div[class*="section"], div[class*="module"], div[class*="curriculum"]'
                            ));

                            // Tìm các phần tử có thể là tiêu đề chương
                            const possibleHeadings = Array.from(document.querySelectorAll(
                                'h1, h2, h3, h4, h5, div.chapter-heading, div.section-heading, ' +
                                'div[class*="chapter-title"], div[class*="section-title"]'
                            ));

                            // Tìm các phần tử có thể là bài giảng
                            const possibleLectures = Array.from(document.querySelectorAll('a[href*="/learn/"]'));

                            // Phân tích cấu trúc để xác định chương và bài giảng
                            const chapters = [];
                            let currentChapter = { title: "Chương không xác định", lectures: [] };

                            // Tìm các chương dựa trên heading
                            possibleHeadings.forEach(heading => {
                                const text = heading.textContent.trim();
                                if (text && text.length < 200) {
                                    if (currentChapter.lectures.length > 0 || chapters.length === 0) {
                                        chapters.push(currentChapter);
                                    }
                                    currentChapter = { title: text, lectures: [] };
                                }
                            });

                            // Thêm chương cuối cùng nếu có
                            if (currentChapter.lectures.length > 0 || chapters.length === 0) {
                                chapters.push(currentChapter);
                            }

                            // Nếu không tìm thấy chương, tạo chương mặc định
                            if (chapters.length === 0) {
                                chapters.push({ title: "Toàn khóa học", lectures: [] });
                            }

                            // Gán bài giảng vào chương
                            let chapterIdx = 0;
                            let allLectures = [];

                            possibleLectures.forEach(lecture => {
                                const title = lecture.textContent.trim();
                                const url = lecture.href;

                                if (title && title !== "Vào học") {
                                    chapterIdx = Math.min(chapterIdx, chapters.length - 1);

                                    chapters[chapterIdx].lectures.push({
                                        title: title,
                                        url: url
                                    });

                                    allLectures.push({
                                        title: title,
                                        url: url,
                                        chapter: chapters[chapterIdx].title
                                    });

                                    // Nếu tiêu đề giống với tiêu đề chương tiếp theo, chuyển chương
                                    if (chapterIdx < chapters.length - 1 && title === chapters[chapterIdx + 1].title) {
                                        chapterIdx++;
                                    }
                                }
                            });

                            return { chapters, allLectures };
                        }

                        return findChapters();
                    """)

                    if chapter_data and 'allLectures' in chapter_data and chapter_data['allLectures']:
                        all_lectures = chapter_data['allLectures']
                        chapters = chapter_data['chapters']
                except Exception as e:
                    self._debug_log(f"Lỗi khi chạy JavaScript để phân tích chương: {e}")

            # Phương pháp 3: Gán bài giảng vào chương dựa trên vị trí
            if not all_lectures:
                # Lấy tất cả bài giảng
                lectures = []
                lecture_elements = driver.find_elements(By.CSS_SELECTOR, "a[href*='/learn/']")

                for elem in lecture_elements:
                    href = elem.get_attribute('href')
                    if href and '/learn/' in href:
                        title = elem.text.strip()
                        if not title:
                            title_elem = elem.find_elements(By.TAG_NAME, 'span')
                            if title_elem:
                                title = title_elem[0].text.strip()

                        # Bỏ qua các bài giảng "Vào học"
                        if title and title != "Vào học":
                            lectures.append({
                                "title": title,
                                "url": href
                            })

                # Nếu tìm thấy bài giảng nhưng không thể phân loại chương
                if lectures:
                    # Chia bài giảng thành các chương dựa trên một số pattern
                    current_chapter = None

                    for lecture in lectures:
                        title = lecture["title"]

                        # Kiểm tra xem đây có phải là tiêu đề chương mới không
                        is_chapter_title = bool(re.search(r'^(chương|phần|module|bài)\s+\d+|^\d+\.\s+', title.lower()))

                        if is_chapter_title:
                            current_chapter = title

                        all_lectures.append({
                            "title": title,
                            "url": lecture["url"],
                            "chapter": current_chapter or "Chưa phân loại"
                        })

                    # Tạo danh sách chương từ các bài giảng
                    chapter_dict = {}
                    for lecture in all_lectures:
                        chapter_title = lecture["chapter"]
                        if chapter_title not in chapter_dict:
                            chapter_dict[chapter_title] = {"title": chapter_title, "lectures": []}

                        chapter_dict[chapter_title]["lectures"].append({
                            "title": lecture["title"],
                            "url": lecture["url"]
                        })

                    chapters = list(chapter_dict.values())

            # Lưu kết quả
            self.chapters = chapters
            return all_lectures

        except Exception as e:
            self._log(f"Lỗi khi trích xuất thông tin chương: {e}")
            if self.debug:
                traceback.print_exc()
            return []

    def scrape_lecture_list(self, course_url):
        """Lấy danh sách bài giảng từ URL khóa học kèm thông tin chương"""
        course_id = self.extract_course_id(course_url)
        if not course_id:
            self._log(f"Lỗi: Định dạng URL khóa học không hợp lệ: {course_url}")
            return None

        # Trích xuất thông tin chương
        lecture_with_chapters = self._extract_chapters(course_url)

        # Khởi tạo cấu trúc kết quả
        result = {
            "data": [],
            "table": "Lecture List",
            "schema_version": "1.0",
            "export_id": f"{course_id}-{int(time.time())}",
            "export_created_at": self.get_iso_time()
        }

        # Nếu đã trích xuất được thông tin từ chương
        if lecture_with_chapters:
            position = 1
            for lecture in lecture_with_chapters:
                result["data"].append({
                    "Position": position,
                    "Lecture Link": lecture["url"],
                    "Lecture Title": lecture["title"],
                    "Extract Date": self.get_iso_time(),
                    "Task Link": "",
                    "Origin URL": course_url,
                    "Lecture List Limit": 100,
                    "Chapter": lecture["chapter"]
                })
                position += 1

            self._log(f"Đã tìm thấy {len(result['data'])} bài giảng từ {len(self.chapters)} chương")
            return result

        # Nếu không trích xuất được thông tin chương, sử dụng phương pháp đơn giản
        try:
            # Phương pháp 1: Thử dùng requests nếu có BeautifulSoup
            lectures = []

            if BS4_AVAILABLE and self.session:
                self._log("Đang thử lấy danh sách bài giảng bằng requests...")

                response = self.session.get(course_url)
                if response.status_code == 200:
                    soup = BeautifulSoup(response.text, 'html.parser')

                    # Tìm các liên kết bài giảng
                    lecture_links = soup.select('a[href*="/learn/"]')

                    if lecture_links:
                        position = 1
                        for link in lecture_links:
                            href = link.get('href', '')
                            if '/learn/' in href:
                                # Đảm bảo URL đầy đủ
                                if not href.startswith('http'):
                                    if href.startswith('/'):
                                        lecture_url = f"https://havamath.vn{href}"
                                    else:
                                        lecture_url = f"https://havamath.vn/{href}"
                                else:
                                    lecture_url = href

                                # Lấy tiêu đề từ text của link hoặc các phần tử lân cận
                                title = link.get_text(strip=True)
                                if not title:
                                    title_elem = link.find('span') or link.find('div')
                                    title = title_elem.get_text(strip=True) if title_elem else f"Bài giảng {position}"

                                # Bỏ qua các bài giảng "Vào học"
                                if title == "Vào học":
                                    continue

                                # Thêm vào danh sách
                                lectures.append({
                                    "Position": position,
                                    "Lecture Link": lecture_url,
                                    "Lecture Title": title,
                                    "Extract Date": self.get_iso_time(),
                                    "Task Link": "",
                                    "Origin URL": course_url,
                                    "Lecture List Limit": 100,
                                    "Chapter": "Chưa phân loại"
                                })
                                position += 1

            # Phương pháp 2: Nếu không tìm thấy bằng requests, dùng Selenium
            if not lectures:
                self._log("Đang sử dụng Selenium để lấy danh sách bài giảng...")
                driver = self._init_driver()

                driver.get(course_url)
                time.sleep(5)  # Đợi trang tải đầy đủ

                # Đợi các phần tử bài giảng xuất hiện
                try:
                    WebDriverWait(driver, 15).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, "a[href*='/learn/']"))
                    )
                except:
                    self._log("Hết thời gian chờ các phần tử bài giảng")

                # Tìm lại các liên kết bài giảng
                lecture_elements = driver.find_elements(By.CSS_SELECTOR, "a[href*='/learn/']")

                position = 1
                for elem in lecture_elements:
                    href = elem.get_attribute('href')
                    if href and '/learn/' in href:
                        title = elem.text.strip()
                        if not title:
                            # Tìm tiêu đề trong các phần tử con
                            title_elem = elem.find_elements(By.TAG_NAME, 'span')
                            if title_elem:
                                title = title_elem[0].text.strip()
                            else:
                                title = f"Bài giảng {position}"

                        # Bỏ qua các bài giảng "Vào học"
                        if title == "Vào học":
                            continue

                        # Thêm vào danh sách
                        lectures.append({
                            "Position": position,
                            "Lecture Link": href,
                            "Lecture Title": title,
                            "Extract Date": self.get_iso_time(),
                            "Task Link": "",
                            "Origin URL": course_url,
                            "Lecture List Limit": 100,
                            "Chapter": "Chưa phân loại"
                        })
                        position += 1

            result["data"] = lectures

            if not lectures:
                self._log(f"Cảnh báo: Không tìm thấy bài giảng nào cho {course_url}")
            else:
                self._log(f"Đã tìm thấy {len(lectures)} bài giảng")

            # Thử phân loại chương bằng phương pháp đơn giản
            self._classify_lectures_by_title(result["data"])

            return result

        except Exception as e:
            self._log(f"Lỗi khi lấy danh sách bài giảng: {e}")
            if self.debug:
                traceback.print_exc()
            return None

    def _classify_lectures_by_title(self, lectures):
        """Phân loại bài giảng vào chương dựa trên tiêu đề"""
        current_chapter = "Chưa phân loại"
        chapter_pattern = re.compile(r'^(chương|phần|module|unit|section|bài)\s+\d+|^\d+\.\s+', re.IGNORECASE)

        for lecture in lectures:
            title = lecture.get("Lecture Title", "")

            # Kiểm tra xem đây có phải là tiêu đề chương mới không
            if chapter_pattern.search(title):
                current_chapter = title

            lecture["Chapter"] = current_chapter

    def extract_youtube_url(self, lecture_url, worker_id=None):
        """Trích xuất URL YouTube từ trang bài giảng"""
        driver = self._init_driver(worker_id)

        try:
            driver.get(lecture_url)
            time.sleep(self.wait_time)  # Đợi trang tải với thời gian chờ cấu hình

            # Phương pháp 1: Tìm iframe YouTube
            youtube_iframes = driver.find_elements(By.CSS_SELECTOR, "iframe[src*='youtube']")
            for iframe in youtube_iframes:
                src = iframe.get_attribute("src")
                if src and 'youtube.com' in src:
                    youtube_id = self._extract_youtube_id(src)
                    if youtube_id:
                        return f"https://youtu.be/{youtube_id}"

            # Phương pháp 2: Tìm div có thuộc tính data-youtube-id
            youtube_divs = driver.find_elements(By.CSS_SELECTOR, "[data-youtube-id]")
            for div in youtube_divs:
                youtube_id = div.get_attribute("data-youtube-id")
                if youtube_id:
                    return f"https://youtu.be/{youtube_id}"

            # Phương pháp 3: Tìm liên kết YouTube
            youtube_links = driver.find_elements(By.CSS_SELECTOR, "a[href*='youtube.com'], a[href*='youtu.be']")
            for link in youtube_links:
                href = link.get_attribute("href")
                if href and ('youtube.com' in href or 'youtu.be' in href):
                    youtube_id = self._extract_youtube_id(href)
                    if youtube_id:
                        return f"https://youtu.be/{youtube_id}"

            # Phương pháp 4: Tìm trong nguồn trang
            page_source = driver.page_source

            # Kiểm tra xem có từ khóa YouTube không
            if "youtube.com/embed" in page_source or "youtu.be" in page_source:
                patterns = [
                    r'https://youtu\.be/([a-zA-Z0-9_-]{11})',
                    r'https://www\.youtube\.com/watch\?v=([a-zA-Z0-9_-]{11})',
                    r'https://www\.youtube\.com/embed/([a-zA-Z0-9_-]{11})',
                    r'youtube\.com/embed/([a-zA-Z0-9_-]{11})',
                    r'youtube_id["\s:=]+["\']([a-zA-Z0-9_-]{11})',
                    r'youtubeId["\s:=]+["\']([a-zA-Z0-9_-]{11})',
                    r'videoId["\s:=]+["\']([a-zA-Z0-9_-]{11})',
                    r'video-id="([a-zA-Z0-9_-]{11})"',
                    r'data-video-id="([a-zA-Z0-9_-]{11})"',
                    r'youtube\.com/v/([a-zA-Z0-9_-]{11})',
                    r'youtube\.com/vi/([a-zA-Z0-9_-]{11})',
                    r'/embed/([a-zA-Z0-9_-]{11})',
                    r'youtu\.be/([a-zA-Z0-9_-]{11})',
                    r'data-youtube-id="([a-zA-Z0-9_-]{11})"',
                ]

                for pattern in patterns:
                    matches = re.findall(pattern, page_source)
                    if matches:
                        youtube_id = matches[0]
                        return f"https://youtu.be/{youtube_id}"

            # Phương pháp 5: Tìm bằng JavaScript
            try:
                youtube_elements = driver.execute_script("""
                    // Tìm tất cả phần tử có thuộc tính chứa youtube
                    var elements = Array.from(document.querySelectorAll('*')).filter(
                        el => {
                            for (var i = 0; i < el.attributes.length; i++) {
                                var attr = el.attributes[i];
                                if (attr.value && (
                                    attr.value.includes('youtube.com') || 
                                    attr.value.includes('youtu.be') || 
                                    attr.value.match(/[a-zA-Z0-9_-]{11}/)
                                )) {
                                    return true;
                                }
                            }

                            // Kiểm tra cả các phần tử iframe 
                            if (el.tagName === 'IFRAME' && el.src && 
                                (el.src.includes('youtube.com') || el.src.includes('youtu.be'))) {
                                return true;
                            }

                            return false;
                        }
                    );

                    var result = {};

                    // Lấy thông tin từ các phần tử
                    if (elements.length > 0) {
                        result.elements = elements.map(e => {
                            var info = { tagName: e.tagName };

                            // Lấy src nếu là iframe
                            if (e.tagName === 'IFRAME' && e.src) {
                                info.src = e.src;
                            }

                            // Lấy các thuộc tính khác liên quan đến YouTube
                            for (var i = 0; i < e.attributes.length; i++) {
                                var attr = e.attributes[i];
                                if (attr.value && (
                                    attr.value.includes('youtube.com') || 
                                    attr.value.includes('youtu.be') || 
                                    attr.value.match(/[a-zA-Z0-9_-]{11}/)
                                )) {
                                    info[attr.name] = attr.value;
                                }
                            }

                            return info;
                        });
                    }

                    return result;
                """)

                if youtube_elements and 'elements' in youtube_elements:
                    for element in youtube_elements['elements']:
                        # Kiểm tra các thuộc tính
                        for key, value in element.items():
                            if key != 'tagName':
                                youtube_id = self._extract_youtube_id(value)
                                if youtube_id:
                                    return f"https://youtu.be/{youtube_id}"
            except Exception as e:
                if self.debug:
                    self._debug_log(f"Lỗi khi chạy JavaScript để tìm YouTube: {e}")

            return None

        except Exception as e:
            self._log(f"Lỗi khi trích xuất URL YouTube từ {lecture_url}: {e}")
            if self.debug:
                traceback.print_exc()
            return None
        finally:
            # Nếu không sử dụng lại driver, đóng driver
            if not self.reuse_driver and worker_id is not None:
                if worker_id in self.worker_drivers:
                    self.worker_drivers[worker_id].quit()
                    del self.worker_drivers[worker_id]

    def _extract_youtube_id(self, url):
        """Trích xuất ID YouTube từ URL"""
        if not url:
            return None

        patterns = [
            r'(?:youtube\.com/watch\?v=|youtu\.be/|youtube\.com/embed/)([a-zA-Z0-9_-]{11})',
            r'youtube_id["\s:=]+["\']([a-zA-Z0-9_-]{11})',
            r'youtubeId["\s:=]+["\']([a-zA-Z0-9_-]{11})',
            r'videoId["\s:=]+["\']([a-zA-Z0-9_-]{11})',
            r'video-id="([a-zA-Z0-9_-]{11})"',
            r'data-video-id="([a-zA-Z0-9_-]{11})"',
            r'youtube\.com/v/([a-zA-Z0-9_-]{11})',
            r'youtube\.com/vi/([a-zA-Z0-9_-]{11})',
            r'youtu\.be/([a-zA-Z0-9_-]{11})',
            r'data-youtube-id="([a-zA-Z0-9_-]{11})"',
        ]

        for pattern in patterns:
            match = re.search(pattern, url)
            if match:
                return match.group(1)

        # Nếu không khớp với pattern nào, kiểm tra xem URL có phải là ID YouTube không
        if re.match(r'^[a-zA-Z0-9_-]{11}$', url):
            return url

        return None

    def process_lecture(self, lecture, index, total):
        """Xử lý một bài giảng"""
        try:
            worker_id = f"worker-{index}"
            lecture_url = lecture.get('Lecture Link')
            title = lecture.get('Lecture Title', f"Bài giảng {index + 1}")

            if not lecture_url:
                return lecture

            self._log(f"[{index + 1}/{total}] Đang xử lý: {title}")

            # Nếu đã có Video URL và không phải rỗng, bỏ qua
            if lecture.get('Video URL') and lecture.get('Video URL').startswith('https://youtu.be/'):
                self._log(f"  Đã có URL YouTube: {lecture.get('Video URL')}")
                return lecture

            youtube_url = self.extract_youtube_url(lecture_url, worker_id)

            if youtube_url:
                lecture['Video URL'] = youtube_url
                self._log(f"  Đã tìm thấy URL YouTube: {youtube_url}")
            else:
                lecture['Video URL'] = ""
                self._log("  Không tìm thấy URL YouTube")

            return lecture
        except Exception as e:
            self._log(f"Lỗi khi xử lý bài giảng: {e}")
            if self.debug:
                traceback.print_exc()
            # Trả về lecture gốc nếu có lỗi
            return lecture

    def update_lecture_data_with_videos_multithreaded(self, lecture_data):
        """Cập nhật dữ liệu bài giảng với URL YouTube sử dụng đa luồng"""
        if not lecture_data or 'data' not in lecture_data:
            self._log("Lỗi: Dữ liệu bài giảng không hợp lệ")
            return lecture_data

        lectures = lecture_data['data']
        total = len(lectures)
        self._log(f"Đang trích xuất URL YouTube cho {total} bài giảng với {self.max_workers} luồng...")

        # Sử dụng ThreadPoolExecutor cho đa luồng
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Đặt futures cho từng công việc
            future_to_index = {
                executor.submit(self.process_lecture, lecture, i, total): i
                for i, lecture in enumerate(lectures)
            }

            # Thu thập kết quả khi các công việc hoàn thành
            for future in as_completed(future_to_index):
                index = future_to_index[future]
                try:
                    # Lấy kết quả từ future
                    updated_lecture = future.result()
                    # Cập nhật lại lecture trong danh sách
                    lectures[index] = updated_lecture
                except Exception as e:
                    self._log(f"Lỗi khi xử lý bài giảng #{index + 1}: {e}")

        # Cập nhật lại dữ liệu
        lecture_data['data'] = lectures

        return lecture_data

    def simplify_lecture_data(self, lecture_data):
        """Chuyển đổi dữ liệu sang định dạng đơn giản (chỉ có title, videoUrl và chapter)"""
        if not lecture_data or 'data' not in lecture_data:
            self._log("Lỗi: Dữ liệu bài giảng không hợp lệ")
            return {"lectures": []}

        # Danh sách bài giảng đã làm sạch
        simplified_lectures = []

        # Tập hợp để theo dõi các bài học đã thêm vào
        added_lectures = set()

        for lecture in lecture_data['data']:
            title = lecture.get('Lecture Title', '')
            video_url = lecture.get('Video URL', '')
            chapter = lecture.get('Chapter', 'Chưa phân loại')

            # Bỏ qua các bài học có tiêu đề "Vào học"
            if title == "Vào học":
                continue

            # Tạo một định danh duy nhất cho bài học
            lecture_id = f"{title}_{video_url}"

            # Nếu bài học chưa được thêm vào, thêm vào danh sách làm sạch
            if lecture_id not in added_lectures:
                simplified_lectures.append({
                    "title": title,
                    "videoUrl": video_url,
                    "chapter": chapter
                })
                added_lectures.add(lecture_id)

        # Tạo dữ liệu đầu ra
        output_data = {
            "lectures": simplified_lectures
        }

        return output_data

    def process_full_workflow(self, course_url, output_file=None, skip_videos=False):
        """Thực hiện toàn bộ quy trình từ URL khóa học đến trích xuất video"""
        # Bước 1: Lấy danh sách bài giảng
        lecture_data = self.scrape_lecture_list(course_url)

        if not lecture_data or not lecture_data.get('data'):
            self._log("Không thể lấy danh sách bài giảng, hủy bỏ")
            return False

        # Nếu chỉ lấy danh sách, không trích xuất video
        if skip_videos:
            if self.simplified_output:
                output_data = self.simplify_lecture_data(lecture_data)
            else:
                output_data = lecture_data

            if output_file is None:
                course_id = self.extract_course_id(course_url) or 'course'
                output_file = f"{course_id}_lectures.json"

            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(output_data, f, ensure_ascii=False, indent=2)

            self._log(f"Đã lưu danh sách bài giảng vào {output_file}")
            return True

        # Bước 2: Trích xuất URL YouTube
        updated_data = self.update_lecture_data_with_videos_multithreaded(lecture_data)

        # Bước 3: Chuyển đổi sang định dạng đơn giản nếu cần
        if self.simplified_output:
            result_data = self.simplify_lecture_data(updated_data)
        else:
            result_data = updated_data

        # Bước 4: Lưu kết quả
        if output_file is None:
            course_id = self.extract_course_id(course_url) or 'course'
            output_file = f"{course_id}_videos.json"

        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(result_data, f, ensure_ascii=False, indent=2)

        self._log(f"Đã lưu dữ liệu thành công vào {output_file}")
        return True

    def process_existing_json(self, json_file, output_file=None, skip_videos=False):
        """Xử lý file JSON đã có sẵn"""
        try:
            with open(json_file, 'r', encoding='utf-8') as f:
                lecture_data = json.load(f)

            # Kiểm tra xem đây là định dạng đơn giản hay không
            if 'lectures' in lecture_data:
                # Chuyển đổi sang định dạng cũ
                old_format_data = {
                    "data": [],
                    "table": "Lecture List",
                    "schema_version": "1.0",
                    "export_id": f"import-{int(time.time())}",
                    "export_created_at": self.get_iso_time()
                }

                for i, lecture in enumerate(lecture_data['lectures']):
                    old_format_data['data'].append({
                        "Position": i + 1,
                        "Lecture Link": f"https://havamath.vn/unknown/link/{i + 1}",
                        "Lecture Title": lecture.get('title', f"Bài giảng {i + 1}"),
                        "Extract Date": self.get_iso_time(),
                        "Task Link": "",
                        "Origin URL": "",
                        "Lecture List Limit": 100,
                        "Video URL": lecture.get('videoUrl', ''),
                        "Chapter": lecture.get('chapter', 'Chưa phân loại')
                    })

                lecture_data = old_format_data

            # Nếu chỉ cần file mà không cần trích xuất video
            if skip_videos:
                if self.simplified_output:
                    output_data = self.simplify_lecture_data(lecture_data)
                else:
                    output_data = lecture_data

                save_path = output_file if output_file else json_file
                with open(save_path, 'w', encoding='utf-8') as f:
                    json.dump(output_data, f, ensure_ascii=False, indent=2)

                self._log(f"Đã xử lý {json_file} không trích xuất video và lưu vào {save_path}")
                return True

            # Cập nhật với URL YouTube
            updated_data = self.update_lecture_data_with_videos_multithreaded(lecture_data)

            # Chuyển đổi sang định dạng đơn giản nếu cần
            if self.simplified_output:
                result_data = self.simplify_lecture_data(updated_data)
            else:
                result_data = updated_data

            # Lưu kết quả
            save_path = output_file if output_file else json_file
            with open(save_path, 'w', encoding='utf-8') as f:
                json.dump(result_data, f, ensure_ascii=False, indent=2)

            self._log(f"Đã xử lý {json_file} và lưu vào {save_path}")
            return True

        except Exception as e:
            self._log(f"Lỗi khi xử lý file JSON {json_file}: {e}")
            if self.debug:
                traceback.print_exc()
            return False

    def close(self):
        """Đóng tất cả các trình duyệt và dọn dẹp tài nguyên"""
        if self.driver:
            self.driver.quit()
            self.driver = None

        # Đóng các worker drivers
        for worker_id, driver in list(self.worker_drivers.items()):
            try:
                driver.quit()
            except:
                pass

        self.worker_drivers.clear()


def main():
    parser = argparse.ArgumentParser(description='Công cụ trích xuất URL YouTube từ Havamath')

    # Nhóm tùy chọn đầu vào
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--url', help='URL khóa học để xử lý')
    group.add_argument('--json', help='File JSON hiện có để xử lý')

    # Các tùy chọn khác
    parser.add_argument('--cookies', help='Đường dẫn đến file cookies JSON')
    parser.add_argument('--output', help='Đường dẫn file đầu ra')
    parser.add_argument('--no-headless', action='store_true', help='Hiển thị trình duyệt khi chạy')
    parser.add_argument('--skip-videos', action='store_true',
                        help='Chỉ lấy danh sách bài giảng, không trích xuất URL video')
    parser.add_argument('--quiet', action='store_true', help='Không hiển thị thông báo tiến trình')
    parser.add_argument('--debug', action='store_true', help='Hiển thị thông tin debug')
    parser.add_argument('--wait-time', type=int, default=10, help='Thời gian chờ trang tải (giây)')
    parser.add_argument('--threads', type=int, default=4, help='Số luồng xử lý đồng thời')
    parser.add_argument('--full-output', action='store_true', help='Xuất đầy đủ thông tin, không đơn giản hóa')
    parser.add_argument('--reuse-browser', action='store_true',
                        help='Tái sử dụng trình duyệt cho các luồng (giảm tài nguyên)')

    args = parser.parse_args()

    try:
        extractor = HavamathExtractor(
            cookies_file=args.cookies,
            headless=not args.no_headless,
            verbose=not args.quiet,
            wait_time=args.wait_time,
            debug=args.debug,
            max_workers=args.threads,
            simplified_output=not args.full_output,
            reuse_driver=args.reuse_browser
        )

        if args.url:
            success = extractor.process_full_workflow(args.url, args.output, args.skip_videos)
        elif args.json:
            success = extractor.process_existing_json(args.json, args.output, args.skip_videos)

        if success:
            print("Hoàn thành tác vụ thành công!")
        else:
            print("Thất bại khi thực hiện tác vụ.")
            return 1

        return 0

    except KeyboardInterrupt:
        print("\nĐã hủy bởi người dùng")
        return 130
    except Exception as e:
        print(f"Lỗi không mong đợi: {e}")
        if args.debug:
            traceback.print_exc()
        return 1
    finally:
        # Luôn đóng trình duyệt nếu đã khởi tạo
        if 'extractor' in locals():
            extractor.close()


if __name__ == "__main__":
    exit(main())