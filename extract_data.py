import pandas as pd
import requests
from bs4 import BeautifulSoup
from scrapingbee import ScrapingBeeClient
from concurrent.futures import ThreadPoolExecutor, as_completed
from tenacity import retry, stop_after_attempt, wait_exponential

# Load URLs from CSV
df = pd.read_csv("urls.csv")
urls = df["loc"].tolist()
client = ScrapingBeeClient(api_key="YOUR_TOKEN")


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
def fetch_with_retry(url):
    """Fetch the URL with retries."""
    # response = client.get(
    #     url,
    #     params={
    #         'premium_proxy': True,  # Use premium proxies for tough sites
    #         'country_code': 'gb',
    #         "block_resources": True,  # Block images and CSS to speed up loading
    #         'device': 'desktop',
    #     }
    # )
    response = requests.get(url)
    response.raise_for_status()
    return response


def extract_text_from_url(url):
    print(f"Processing {url}")
    """Fetch and extract text from a single URL."""
    try:
        response = fetch_with_retry(url)
        soup = BeautifulSoup(response.content, "html.parser")

        text = soup.get_text(separator="\n", strip=True)
        return text
    except requests.exceptions.RequestException as e:
        print(f"Error fetching {url}: {e}")
        return ""


def main():
    all_texts = []

    # Use ThreadPoolExecutor to handle threading
    with ThreadPoolExecutor(max_workers=5) as executor:
        # Submit all URL processing tasks to the executor
        futures = {executor.submit(extract_text_from_url, url): url for url in urls}

        # Collect results as they complete
        for future in as_completed(futures):
            result = future.result()
            all_texts.append(result)

    # Save extracted texts to a file, ensuring newlines are correctly written
    with open("extracted_texts.txt", "w", encoding="utf-8") as file:
        for text in all_texts:
            file.write(text + "\n\n")

    print("Text extraction completed successfully!")


if __name__ == "__main__":
    main()
