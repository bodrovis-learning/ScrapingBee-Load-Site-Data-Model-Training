from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import pandas as pd
import requests
from bs4 import BeautifulSoup
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

INPUT_CSV = Path("urls.csv")
OUTPUT_FILE = Path("extracted_texts.txt")
URL_COLUMN = "loc"
REQUEST_TIMEOUT = 30
MAX_WORKERS = 5


def load_urls(csv_path: Path) -> list[str]:
    """Load URLs from the sitemap CSV file."""
    try:
        df = pd.read_csv(csv_path)
    except FileNotFoundError:
        print(f"Input file not found: {csv_path}")
        return []
    except pd.errors.EmptyDataError:
        print(f"Input file is empty: {csv_path}")
        return []
    except pd.errors.ParserError as e:
        print(f"Failed to parse CSV file {csv_path}: {e}")
        return []

    if URL_COLUMN not in df.columns:
        print(f"Missing required column: {URL_COLUMN}")
        return []

    return [
        str(url).strip()
        for url in df[URL_COLUMN].dropna()
        if str(url).strip() and str(url).strip().lower() != "n/a"
    ]


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    retry=retry_if_exception_type(requests.RequestException),
    reraise=True,
)
def fetch_with_retry(url: str) -> requests.Response:
    """Fetch a URL with retries."""
    response = requests.get(url, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()
    return response


def fetch_page_text(url: str) -> str | None:
    """Fetch a page and extract readable text from its HTML."""
    try:
        response = fetch_with_retry(url)
    except requests.RequestException as e:
        print(f"Error fetching {url}: {e}")
        return None

    content_type = response.headers.get("Content-Type", "")

    if content_type and "text/html" not in content_type:
        print(f"Skipping non-HTML page: {url}")
        return None

    soup = BeautifulSoup(response.text, "html.parser")

    # Remove elements that usually do not contain useful training text.
    for tag in soup(["script", "style", "noscript"]):
        tag.decompose()

    return soup.get_text(separator="\n", strip=True)


def save_texts(texts: list[str], output_path: Path) -> bool:
    """Save extracted texts to a UTF-8 encoded text file."""
    try:
        with output_path.open("w", encoding="utf-8") as file:
            for text in texts:
                file.write(text + "\n\n")
    except OSError as e:
        print(f"Failed to write output file {output_path}: {e}")
        return False

    return True


def main() -> None:
    urls = load_urls(INPUT_CSV)

    if not urls:
        print("No URLs found. Nothing to extract.")
        return

    all_texts: list[str] = []

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(fetch_page_text, url): url
            for url in urls
        }

        for future in as_completed(futures):
            url = futures[future]

            try:
                text = future.result()
            except Exception as e:
                print(f"Unexpected error processing {url}: {e}")
                continue

            if text:
                all_texts.append(text)

    if save_texts(all_texts, OUTPUT_FILE):
        print("Text extraction completed successfully!")


if __name__ == "__main__":
    main()