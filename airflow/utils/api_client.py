"""API Client for Open Brewery DB."""
import logging
import time
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)


class BreweryAPIClient:
    """Client for fetching brewery data from Open Brewery DB API."""

    def __init__(self, base_url="https://api.openbrewerydb.org/v1", page_size=200):
        self.base_url = base_url
        self.page_size = min(page_size, 200)
        self.session = self._create_session()

    def _create_session(self):
        """Create requests session with retry logic."""
        session = requests.Session()
        retry = Retry(
            total=3,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["HEAD", "GET", "OPTIONS"],
            backoff_factor=1,
        )
        session.mount("http://", HTTPAdapter(max_retries=retry))
        session.mount("https://", HTTPAdapter(max_retries=retry))
        return session

    def fetch_all_breweries(self):
        """Fetch all breweries using pagination."""
        all_breweries = []
        page = 1

        logger.info(f"Fetching breweries (page size: {self.page_size})")

        while True:
            breweries = self._fetch_page(page)
            if not breweries:
                break

            all_breweries.extend(breweries)
            logger.info(f"Page {page}: {len(breweries)} breweries (total: {len(all_breweries)})")

            if len(breweries) < self.page_size:
                break

            page += 1
            time.sleep(0.1)

        logger.info(f"Total breweries fetched: {len(all_breweries)}")
        return all_breweries

    def _fetch_page(self, page):
        """Fetch single page of breweries."""
        url = f"{self.base_url}/breweries"
        params = {"page": page, "per_page": self.page_size}
        response = self.session.get(url, params=params, timeout=30)
        response.raise_for_status()
        return response.json()

    def close(self):
        self.session.close()
