import csv
from pathlib import Path

import requests
from bs4 import BeautifulSoup as Soup

from typing import Final

# Constants for the attributes to be extracted from the sitemap.
ATTRS: Final[tuple[str, ...]] = ("loc", "lastmod", "priority")

def parse_sitemap(
    url: str,
    csv_filename: str = "urls.csv",
    visited: set[str] | None = None,
) -> bool:
    """Parse the sitemap at the given URL and append the data to a CSV file."""
    if not url:
        print("No sitemap URL provided.")
        return False

    if visited is None:
        visited = set()

    url = url.strip()

    # Avoid processing the same sitemap more than once.
    if url in visited:
        return True

    visited.add(url)

    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
    except requests.RequestException as e:
        print(f"Failed to fetch sitemap {url}: {e}")
        return False

    soup = Soup(response.content, "xml")

    success = True

    # Recursively parse nested sitemaps.
    for sitemap in soup.find_all("sitemap"):
        loc = sitemap.find("loc")

        if loc and loc.text:
            success = parse_sitemap(
                loc.text.strip(),
                csv_filename,
                visited,
            ) and success

    # Find all URL entries in the sitemap.
    urls = soup.find_all("url")

    rows: list[list[str]] = []
    for url_entry in urls:
        row = []

        for attr in ATTRS:
            found_attr = url_entry.find(attr)
            row.append(found_attr.text.strip() if found_attr else "n/a")

        rows.append(row)

    if not rows:
        return success

    # Save the CSV file in the same directory as the script.
    csv_path = Path(__file__).resolve().parent / csv_filename
    file_exists = csv_path.exists()

    try:
        with csv_path.open("a", newline="", encoding="utf-8") as csvfile:
            writer = csv.writer(csvfile)

            if not file_exists:
                writer.writerow(ATTRS)

            writer.writerows(rows)
    except OSError as e:
        print(f"Failed to write sitemap data to {csv_path}: {e}")
        return False

    return success


if __name__ == "__main__":
    parse_sitemap("https://bodrovis.tech/sitemap.xml")