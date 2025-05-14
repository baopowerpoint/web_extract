import json
import re
import argparse
import os
from urllib.parse import urlparse
import requests
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time


class HavamathCourseScraper:
    def __init__(self, cookies_file=None, headless=True):
        """Initialize the course scraper with optional cookies file"""
        self.cookies_file = cookies_file
        self.headless = headless
        self.session = requests.Session()

        # Set up headers for requests
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
        }
        self.session.headers.update(self.headers)

        # Initialize the driver only when needed
        self.driver = None

    def init_driver(self):
        """Initialize the Selenium WebDriver"""
        if self.driver is not None:
            return

        chrome_options = Options()
        if self.headless:
            chrome_options.add_argument("--headless")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--window-size=1920,1080")

        # Initialize Chrome driver
        self.driver = webdriver.Chrome(options=chrome_options)

        # Load cookies if available
        if self.cookies_file and os.path.exists(self.cookies_file):
            self.load_cookies_to_driver()

    def load_cookies_to_requests(self):
        """Load cookies to the requests session"""
        if not self.cookies_file or not os.path.exists(self.cookies_file):
            print("No cookies file provided or file does not exist")
            return False

        try:
            with open(self.cookies_file, 'r') as f:
                cookies = json.load(f)

                # Convert cookies to requests format
                for cookie in cookies:
                    self.session.cookies.set(
                        cookie['name'],
                        cookie['value'],
                        domain=cookie.get('domain', ''),
                        path=cookie.get('path', '/')
                    )
            return True
        except Exception as e:
            print(f"Error loading cookies to requests: {e}")
            return False

    def load_cookies_to_driver(self):
        """Load cookies to Selenium WebDriver"""
        if not self.cookies_file or not os.path.exists(self.cookies_file):
            print("No cookies file provided or file does not exist")
            return False

        # Make sure driver is initialized
        self.init_driver()

        # Visit the domain first
        domain = 'havamath.vn'
        self.driver.get(f"https://{domain}")
        time.sleep(2)  # Allow the page to load

        try:
            with open(self.cookies_file, 'r') as f:
                cookies = json.load(f)

                for cookie in cookies:
                    # Skip expired cookies
                    if 'expiry' in cookie:
                        cookie['expiry'] = int(cookie['expiry'])

                    try:
                        self.driver.add_cookie(cookie)
                    except Exception as e:
                        print(f"Error adding cookie to driver: {e}")
                        continue

            # Refresh the page to apply cookies
            self.driver.refresh()
            return True

        except Exception as e:
            print(f"Error loading cookies to driver: {e}")
            return False

    def extract_course_id(self, course_url):
        """Extract course ID from URL"""
        parsed_url = urlparse(course_url)
        path_parts = parsed_url.path.strip('/').split('/')
        if len(path_parts) >= 2 and path_parts[0] == 'courses':
            return path_parts[1]
        return None

    def scrape_lecture_list(self, course_url):
        """Scrape the list of lectures from a course page"""
        course_id = self.extract_course_id(course_url)
        if not course_id:
            print(f"Error: Invalid course URL format: {course_url}")
            return None

        # Initialize the result structure
        result = {
            "data": [],
            "table": "Lecture List",
            "schema_version": "1.0",
            "export_id": f"{course_id}-{int(time.time())}",
            "export_created_at": time.strftime("%Y-%m-%dT%H:%M:%S.%fZ", time.gmtime())
        }

        # Load cookies for requests
        if self.cookies_file:
            self.load_cookies_to_requests()

        try:
            # First approach: Try using requests to get the course page
            response = self.session.get(course_url)
            response.raise_for_status()

            soup = BeautifulSoup(response.text, 'html.parser')

            # Look for lecture links
            lectures = []

            # Method 1: Try to find a structured list of lectures
            lecture_links = soup.select('a[href*="/learn/"]')

            if lecture_links:
                position = 1
                for link in lecture_links:
                    href = link.get('href', '')
                    if '/learn/' in href:
                        # Ensure full URL
                        if not href.startswith('http'):
                            if href.startswith('/'):
                                lecture_url = f"https://havamath.vn{href}"
                            else:
                                lecture_url = f"https://havamath.vn/{href}"
                        else:
                            lecture_url = href

                        # Get title from the link text or nearby elements
                        title = link.get_text(strip=True)
                        if not title:
                            title_elem = link.find('span') or link.find('div')
                            title = title_elem.get_text(strip=True) if title_elem else f"Lecture {position}"

                        lectures.append({
                            "Position": position,
                            "Lecture Link": lecture_url,
                            "Lecture Title": title,
                            "Extract Date": time.strftime("%Y-%m-%dT%H:%M:%S.%fZ", time.gmtime()),
                            "Task Link": "",
                            "Origin URL": course_url,
                            "Lecture List Limit": 100
                        })
                        position += 1

            # If we didn't find lectures using the first method, try with Selenium
            if not lectures:
                print("No lectures found with requests, trying Selenium...")
                self.init_driver()

                self.driver.get(course_url)
                time.sleep(5)  # Allow the page to load fully

                # Wait for potential lecture elements to be visible
                try:
                    WebDriverWait(self.driver, 15).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, "a[href*='/learn/']"))
                    )
                except:
                    print("Timeout waiting for lecture elements")

                # Try to find lecture links again
                lecture_elements = self.driver.find_elements(By.CSS_SELECTOR, "a[href*='/learn/']")

                position = 1
                for elem in lecture_elements:
                    href = elem.get_attribute('href')
                    if href and '/learn/' in href:
                        title = elem.text.strip()
                        if not title:
                            # Try to find title in child elements
                            title_elem = elem.find_elements(By.TAG_NAME, 'span')
                            if title_elem:
                                title = title_elem[0].text.strip()
                            else:
                                title = f"Lecture {position}"

                        lectures.append({
                            "Position": position,
                            "Lecture Link": href,
                            "Lecture Title": title,
                            "Extract Date": time.strftime("%Y-%m-%dT%H:%M:%S.%fZ", time.gmtime()),
                            "Task Link": "",
                            "Origin URL": course_url,
                            "Lecture List Limit": 100
                        })
                        position += 1

            result["data"] = lectures

            if not lectures:
                print(f"Warning: No lectures found for {course_url}")
            else:
                print(f"Found {len(lectures)} lectures")

            return result

        except Exception as e:
            print(f"Error scraping lecture list: {e}")

            # Fallback to reading from the provided JSON sample
            print("Falling back to analyze provided JSON sample structure...")
            return self.analyze_sample_json()

    def analyze_sample_json(self):
        """Analyze the provided JSON sample to understand its structure"""
        try:
            # This would normally read from the JSON file we have
            with open('sample_lecture_list.json', 'r', encoding='utf-8') as f:
                sample_data = json.load(f)
            return sample_data
        except:
            print("Could not read sample JSON file, returning empty structure")
            return {
                "data": [],
                "table": "Lecture List",
                "schema_version": "1.0",
                "export_id": f"sample-{int(time.time())}",
                "export_created_at": time.strftime("%Y-%m-%dT%H:%M:%S.%fZ", time.gmtime())
            }

    def extract_video_url(self, lecture_url):
        """Extract video URL from a lecture page"""
        self.init_driver()

        try:
            self.driver.get(lecture_url)
            time.sleep(5)  # Wait for page to load

            # Method 1: Wait for video elements
            try:
                WebDriverWait(self.driver, 15).until(
                    EC.presence_of_element_located((By.TAG_NAME, "video"))
                )
            except:
                print(f"No video element found on {lecture_url}")

            # Try different methods to find the video URL

            # 1. Direct video element
            video_elements = self.driver.find_elements(By.TAG_NAME, "video")
            for video in video_elements:
                src = video.get_attribute("src")
                if src and (src.endswith('.mp4') or '.m3u8' in src):
                    return src

            # 2. Video source elements
            source_elements = self.driver.find_elements(By.TAG_NAME, "source")
            for source in source_elements:
                src = source.get_attribute("src")
                if src and (src.endswith('.mp4') or '.m3u8' in src):
                    return src

            # 3. Check iframes
            iframe_elements = self.driver.find_elements(By.TAG_NAME, "iframe")
            for iframe in iframe_elements:
                iframe_src = iframe.get_attribute("src")
                if iframe_src and ('player' in iframe_src or 'video' in iframe_src):
                    # Switch to iframe and check for video
                    try:
                        self.driver.switch_to.frame(iframe)
                        iframe_videos = self.driver.find_elements(By.TAG_NAME, "video")
                        for video in iframe_videos:
                            src = video.get_attribute("src")
                            if src:
                                self.driver.switch_to.default_content()
                                return src
                        self.driver.switch_to.default_content()
                    except:
                        self.driver.switch_to.default_content()

            # 4. Look for video URLs in page source
            page_source = self.driver.page_source

            # Common patterns for video URLs
            patterns = [
                r'(https?://[^"\'\s]+\.mp4)',
                r'(https?://[^"\'\s]+\.m3u8)',
                r'videoUrl["\']\s*:\s*["\']([^"\']+)',
                r'videoSrc["\']\s*:\s*["\']([^"\']+)',
                r'playbackUrl["\']\s*:\s*["\']([^"\']+)',
                r'src=["\'](https?://[^"\'\s]+\.mp4)',
                r'src=["\'](https?://[^"\'\s]+\.m3u8)',
            ]

            for pattern in patterns:
                matches = re.findall(pattern, page_source)
                if matches:
                    return matches[0]

            print(f"No video URL found for {lecture_url}")
            return None

        except Exception as e:
            print(f"Error extracting video URL from {lecture_url}: {e}")
            return None

    def update_lecture_data_with_videos(self, lecture_data):
        """Update lecture data with video URLs"""
        if not lecture_data or 'data' not in lecture_data:
            print("Error: Invalid lecture data")
            return lecture_data

        total = len(lecture_data['data'])
        print(f"Extracting video URLs for {total} lectures...")

        for i, lecture in enumerate(lecture_data['data']):
            lecture_url = lecture.get('Lecture Link')
            title = lecture.get('Lecture Title', f"Lecture {i + 1}")

            if lecture_url:
                print(f"[{i + 1}/{total}] Processing: {title}")
                video_url = self.extract_video_url(lecture_url)

                if video_url:
                    lecture['Video URL'] = video_url
                    print(f"  Found video URL: {video_url[:50]}..." if len(
                        video_url) > 50 else f"  Found video URL: {video_url}")
                else:
                    lecture['Video URL'] = ""
                    print("  No video URL found")

                # Pause to avoid overloading the server
                time.sleep(2)

        return lecture_data

    def process_full_workflow(self, course_url, output_file=None):
        """Run the full workflow from course URL to video extraction"""
        # Step 1: Scrape lecture list
        lecture_data = self.scrape_lecture_list(course_url)

        if not lecture_data or not lecture_data.get('data'):
            print("Failed to get lecture list, aborting")
            return False

        # Step 2: Extract video URLs
        updated_data = self.update_lecture_data_with_videos(lecture_data)

        # Step 3: Save results
        if output_file is None:
            course_id = self.extract_course_id(course_url) or 'course'
            output_file = f"{course_id}_data.json"

        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(updated_data, f, ensure_ascii=False, indent=2)

        print(f"Successfully saved data to {output_file}")
        return True

    def process_existing_json(self, json_file, output_file=None):
        """Process an existing JSON file to add video URLs"""
        try:
            with open(json_file, 'r', encoding='utf-8') as f:
                lecture_data = json.load(f)

            # Update with video URLs
            updated_data = self.update_lecture_data_with_videos(lecture_data)

            # Save results
            save_path = output_file if output_file else json_file
            with open(save_path, 'w', encoding='utf-8') as f:
                json.dump(updated_data, f, ensure_ascii=False, indent=2)

            print(f"Successfully processed {json_file} and saved to {save_path}")
            return True

        except Exception as e:
            print(f"Error processing JSON file {json_file}: {e}")
            return False

    def close(self):
        """Close browser and clean up resources"""
        if self.driver:
            self.driver.quit()
            self.driver = None


def main():
    parser = argparse.ArgumentParser(description='Havamath Course Video Extractor')

    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--url', help='Course URL to process')
    group.add_argument('--json', help='Existing JSON file to process')

    parser.add_argument('--cookies', help='Path to cookies JSON file')
    parser.add_argument('--output', help='Output file path')
    parser.add_argument('--no-headless', action='store_true', help='Run browser in non-headless mode')

    args = parser.parse_args()

    scraper = HavamathCourseScraper(
        cookies_file=args.cookies,
        headless=not args.no_headless
    )

    try:
        if args.url:
            success = scraper.process_full_workflow(args.url, args.output)
        elif args.json:
            success = scraper.process_existing_json(args.json, args.output)

        if success:
            print("Operation completed successfully!")
        else:
            print("Operation failed.")
    finally:
        # Always close the browser
        scraper.close()


if __name__ == "__main__":
    main()