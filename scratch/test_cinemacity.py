
import asyncio
import logging
import sys
import os

# Add root path for imports
root_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, root_path)

from extractors.cinemacity import CinemaCityExtractor

async def test():
    url = "https://cinemacity.cc/movies/1326-ready-or-not-2-here-i-come.html"
    extractor = CinemaCityExtractor(request_headers={})
    try:
        result = await extractor.extract(url)
        print("SUCCESS")
        print(result)
    except Exception as e:
        print(f"FAILED: {e}")
    finally:
        await extractor.close()

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(test())
