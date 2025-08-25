import argparse
import asyncio
import json
import os
import re
from urllib.parse import urlparse, urljoin

import aiofiles
import aiohttp
from bs4 import BeautifulSoup


async def fetch(session, url, proxy):
    """Fetches content from a URL using aiohttp, raising an exception on failure."""
    try:
        async with session.get(url, proxy=proxy) as response:
            response.raise_for_status()
            if "image" in response.headers.get("Content-Type", ""):
                return await response.read()
            return await response.text()
    except aiohttp.ClientError as e:
        raise ValueError(f"Network error for {url}") from e


def extract_article_id(url):
    """Extracts the article ID and formats any trailing number to be four digits."""
    base_id = os.path.splitext(os.path.basename(urlparse(url).path))[0]
    match = re.search(r"(.*)-(\d+)$", base_id)
    if match:
        prefix, number = match.groups()
        return f"{prefix}-{number.zfill(4)}"
    return base_id


async def process_task(session, semaphore, task_info, output_dir, proxy):
    """Processes a single task and reports success or failure."""
    original_url, _, date = task_info[:3]
    article_id = extract_article_id(original_url)

    async with semaphore:
        try:
            if date == "":
                wayback_url = original_url
            else:
                wayback_url = f"https://web.archive.org/web/{date}/{original_url}"

            html = await fetch(session, wayback_url, proxy)

            soup = BeautifulSoup(html, "lxml")

            selectors = [
                "#post-body",
                ".post-body",
                ".entry-content",
                '[id^="post-body"]',
            ]
            content_element = None
            for selector in selectors:
                content_element = soup.select_one(selector)
                if content_element:
                    break

            if not content_element:
                raise ValueError("Could not find a suitable content element")

            # Save text content only if it's meaningful
            text_content = content_element.get_text(separator="\n", strip=True)
            if text_content.strip():
                txt_filename = os.path.join(output_dir, f"{article_id}.txt")
                async with aiofiles.open(txt_filename, "w", encoding="utf-8") as f:
                    await f.write(text_content)

            # Download and save images. A single failure will fail the entire task.
            for i, img_tag in enumerate(content_element.find_all("img")):
                img_url = img_tag.get("src")
                if not img_url:
                    continue

                img_url = urljoin(wayback_url, img_url)
                img_data = await fetch(session, img_url, proxy)

                if img_data:
                    img_path = urlparse(img_url).path
                    img_ext = os.path.splitext(img_path)[1]
                    if not img_ext:
                        img_ext = ".jpg"

                    img_filename = os.path.join(
                        output_dir, f"{article_id}_{i}{img_ext}"
                    )
                    async with aiofiles.open(img_filename, "wb") as f:
                        await f.write(img_data)

            print(f"Article ID: {article_id} - Success")

        except Exception as e:
            print(f"Article ID: {article_id} - Failure: {e}")


async def main():
    """Main function to coordinate the script's execution."""
    parser = argparse.ArgumentParser(
        description="Download articles and images from the Wayback Machine."
    )
    parser.add_argument(
        "task_input", help="Path to the JSON file or a single URL to process."
    )
    parser.add_argument("output_dir", help="Directory to save the output files.")
    parser.add_argument(
        "--concurrency",
        type=int,
        default=5,
        help="Number of concurrent requests.",
    )
    parser.add_argument("--proxy", help="Proxy server URL.", default=None)
    args = parser.parse_args()

    if not os.path.exists(args.output_dir):
        os.makedirs(args.output_dir)

    tasks_data = []
    # Check if the input is a URL or a file path
    if ":" in args.task_input or "//" in args.task_input:
        # It's a URL, create a single task with a wildcard date to get the latest snapshot
        tasks_data = [[args.task_input, "", ""]]
    else:
        # It's a file path, read it
        try:
            with open(args.task_input, "r") as f:
                tasks_data = json.load(f)
        except (FileNotFoundError, json.JSONDecodeError) as e:
            print(f"Error: {e}")
            return

    if not tasks_data:
        print("No tasks to process.")
        return

    semaphore = asyncio.Semaphore(args.concurrency)
    async with aiohttp.ClientSession() as session:
        tasks = [
            process_task(session, semaphore, task_info, args.output_dir, args.proxy)
            for task_info in tasks_data
        ]
        await asyncio.gather(*tasks)


if __name__ == "__main__":
    asyncio.run(main())
