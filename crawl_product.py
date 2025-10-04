import asyncio
import aiohttp
import random
import re
import json
import pandas as pd
from config.logging_config import setup_logger
from bs4 import BeautifulSoup

logger = setup_logger("crawl_data")
BATCH_SIZE = 500
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.1 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:117.0) Gecko/20100101 Firefox/117.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Edge/114.0.1823.82",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.5735.198 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:115.0) Gecko/20100101 Firefox/115.0"
]

def load_product_ids(file_path="DB_UMA.distinct_product_ids.csv"):
    try:
        logger.info(f"Loading file product_id: {file_path}")
        df = pd.read_csv(file_path)
        if "product_id" not in df.columns:
            raise ValueError("CSV file must contain column 'product_id'")
        ids = df["product_id"].dropna().astype(str).tolist()
        logger.info(f"Loaded {len(ids)} product_ids")
        return ids
    except Exception as e:
        logger.error(f"Failed to load product_id file: {e}")
        raise

async def crawl_data(session, product_ids,retries=4, backoff=4):
    url = f"https://www.glamira.vn/catalog/product/view/id/{product_ids}"
    # logger.info(f"Crawling URL: {url}")
    
    for attempt in range(1, retries + 1):
        headers = {"User-Agent": random.choice(USER_AGENTS)}
        try:
            async with session.get(url, headers=headers,timeout=300) as response:
                if response.status == 403:
                    logger.warning(f"Product {product_ids} returned 403, retrying with new user-agent")
                if response.status != 200:
                    logger.warning(f"Product {product_ids} returned status {response.status}")
                    return product_ids, None
                return product_ids, await response.text()
        except asyncio.TimeoutError:
            logger.error(f"Timeout error for product {product_ids}")
        except aiohttp.ClientConnectorError as e:
            logger.error(f"Connection error for product {product_ids}: {e}")
        except aiohttp.ClientResponseError as e:
            logger.error(f"Response error for product {product_ids}: status={e.status}, message={e.message}")
        except aiohttp.ClientPayloadError as e:
            logger.error(f"Payload error for product {product_ids}: {e}")
        except Exception as e:
            logger.exception(f"Unexpected error for product {product_ids}: {e}")
        
        wait_time = backoff * attempt
        logger.info(f"Waiting {wait_time}s before retry {attempt+1} for product {product_ids}")
        await asyncio.sleep(wait_time)
    logger.error(f"Failed to crawl product {product_ids} after {retries} attempts")
    return product_ids, None

def parse_view_item(html):
    soup = BeautifulSoup(html, "html.parser")
    script = soup.find("script", text=re.compile(r"var\s+react_data"))
    if not script:
        return None
    match = re.search(r"var\s+react_data\s*=\s*(\{.*?\});", script.text, re.DOTALL)
    if not match:
        return None
    json_text = match.group(1).rstrip(";")
    json_text = json_text.replace("undefined", "null")
    try:
        data = json.loads(json_text)
        return {
            "product_id": data.get("product_id"),
            "product_name": data.get("name"),
            "product_price": data.get("price"),
            "min_price": data.get("min_price"),
            "max_price": data.get("max_price"),
            "min_price_format": data.get("min_price_format"),
            "max_price_format": data.get("max_price_format")
        }
    except json.JSONDecodeError as e:
        logger.error(f"JSON parse error: {e} | snippet: {json_text[:200]}...")
        return None

def save_record(record, file_path="data/output.csv"):
    df = pd.DataFrame([record])
    df.to_csv(file_path, mode="a", index=False, encoding="utf-8-sig",
              header=not pd.io.common.file_exists(file_path))
    
# --- Main ---
async def main():
    product_ids = load_product_ids("DB_UMA.distinct_product_ids.csv")
    #product_ids = load_product_ids("file_403_new.csv")
    connector =  aiohttp.TCPConnector(limit=5)
    

    async with aiohttp.ClientSession(connector=connector) as session:
        for i in range(0, len(product_ids), BATCH_SIZE):
            batch = product_ids[i:i+BATCH_SIZE]
            tasks = []
            for pid in batch:
                task = crawl_data(session, pid)
                tasks.append(task)
            results = await asyncio.gather(*tasks)
            
            for result in results:
                pid, html = result
                if result is None:
                    logger.warning(f"Product {pid} crawl failed")
                    continue
                try:
                    if html:
                        info = parse_view_item(html)
                        if info:
                            info["product_id"] = pid
                            save_record(info, "data/output.csv")
                            logger.info(f"Crawled {pid}: {info['product_name']}")
                        else:
                            logger.warning(f"No viewItem found for {pid}")
                    else:
                        logger.error(f"Failed to crawl {pid}")
                except Exception as e:
                    logger.error(f"Error crawling {pid}: {e}")


if __name__ == "__main__":
    asyncio.run(main())
