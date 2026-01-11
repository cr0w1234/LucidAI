import os
import json
import unicodedata
from typing import List, Dict, Any
from bs4 import BeautifulSoup

from wp_site_etl.core.config import settings

"""
Designed for: https://www.schoolhealthcenters.org/resource/sbhcs-by-county/

This module is used to parse the HTML table from the SBHC by County page and extract the data into a JSON file.

In other words, this script is hack to handle an edge case
"""

def clean_header(text: str) -> str:
    """
    Remove control characters and leading/trailing whitespace from a header cell text.
    """
    return ''.join(ch for ch in text if unicodedata.category(ch)[0] != 'C').strip()


def extract_html_content(items: List[Dict[str, Any]], target_link: str) -> str:
    """
    Load JSON data from the given path and return the HTML content
    for the entry matching the target link.
    Raises ValueError if no matching content is found.
    """

    for item in items:
        # print("Item: ", item.get('link'))
        if item.get('link').strip() == target_link:
            print("Found item: ", item.get('link'))
            print("Item: ", item)
            return item.get('content', {}).get('rendered', '')

    raise ValueError(f"No HTML found for link: {target_link}")


def parse_tables(html: str) -> List[BeautifulSoup]:
    """
    Parse HTML and return a list of all <table> elements.
    Raises ValueError if none are found.
    """
    soup = BeautifulSoup(html, 'html.parser')
    tables = soup.find_all('table')
    if not tables:
        raise ValueError("No <table> elements found in the HTML")
    return tables


def parse_headers(table: BeautifulSoup) -> List[str]:
    """
    Extract and clean header names from the first row of the table.
    """
    header_cells = table.find('tr').find_all('td')
    headers: List[str] = []

    for cell in header_cells:
        paragraphs = cell.find_all('p', class_='s1')
        combined_text = " ".join(p.get_text(strip=True) for p in paragraphs)
        headers.append(clean_header(combined_text))

    return headers


def parse_rows(tables: List[BeautifulSoup], headers: List[str]) -> List[Dict[str, Any]]:
    """
    Parse all rows from provided tables into a list of dictionaries
    keyed by headers. Skips rows with mismatched column counts.
    """
    parsed_data: List[Dict[str, Any]] = []

    for table in tables:
        for row in table.find_all('tr')[1:]:  # Skip header row
            cells = row.find_all('td')
            if len(cells) != len(headers):
                continue

            entry: Dict[str, Any] = {}
            for idx, (header, cell) in enumerate(zip(headers, cells)):
                p_tag = cell.find(
                    lambda tag: tag.name == 'p'
                    and tag.get('class')
                    and tag.get('class')[0] in ('s2', 's3')
                )

                if p_tag is None:
                    entry[header] = "" if idx < 4 else False
                else:
                    text = p_tag.get_text(strip=True)
                    entry[header] = True if 's3' in p_tag.get('class', []) else text

            parsed_data.append(entry)

    return parsed_data


def construct_sentences(data: List[Dict[str, Any]], headers: List[str]) -> List[str]:
    """
    Construct descriptive sentences from parsed data.
    Lists services offered if they are marked True.
    """
    service_columns = headers[4:]
    sentences: List[str] = []

    for entry in data:
        county = entry.get("County", "")
        center = entry.get("School-Based Health & Wellness Center", "")
        city = entry.get("City", "")
        offered_services = [col for col in service_columns if entry.get(col) is True]

        if offered_services:
            sentence = (
                f"The {center} school-based health and wellness center, "
                f"located in the city of {city} in {county} County, "
                f"provides the following services: {', '.join(offered_services)}."
            )
        else:
            sentence = (
                f"The {center} school-based health and wellness center "
                f"is located in the city of {city} in {county} County. "
                "Specific services offered are not listed."
            )
        sentences.append(sentence)

    return sentences


def main() -> None:
    """
    Main function to extract HTML, parse tables, build sentences, and write results to JSON files.
    """

    json_file = settings.RAW_DATA_DIR / "website-data" / "endpoint-content" / "resource_endpoint_content.json"
    
    target_link = "https://www.schoolhealthcenters.org/resource/sbhcs-by-county/"

    with open(json_file, 'r', encoding='utf-8') as f:
        items: List[Dict[str, Any]] = json.load(f, strict=False)

    html_content = extract_html_content(items, target_link)
    print("HTML content: ", html_content)
    tables = parse_tables(html_content)
    headers = parse_headers(tables[0])
    data = parse_rows(tables, headers)
    sentences = construct_sentences(data, headers)
    sentences_paragraph = " ".join(sentences)

    print("Sentences paragraph: ", sentences_paragraph)

    for item in items:
        if item.get('link') == target_link:
            item['content']['rendered'] = sentences_paragraph

    with open(json_file, 'w', encoding='utf-8') as f:
        json.dump(items, f, ensure_ascii=False, indent=2)


if __name__ == "__main__":
    main()
