import asyncio
import threading
from typing import List

import requests
from aqora_cli import Client

from croissant_rdf._src.croissant_harvester import CroissantHarvester

__author__ = "Angel Dijoux"


class _AsyncRunner:
    def __init__(self):
        self._loop = asyncio.new_event_loop()
        self._thread = threading.Thread(
            target=self._loop.run_forever, daemon=True)
        self._thread.start()

    def run(self, coro):
        future = asyncio.run_coroutine_threadsafe(coro, self._loop)
        return future.result()


class AqoraHarvester(CroissantHarvester):
    api_url = "https://aqora.io"

    def __init__(self):
        super().__init__()
        self._runner = _AsyncRunner()
        self._client = Client()
        self._authenticated = False

    def _ensure_authenticated(self):
        if not self._authenticated:
            self._runner.run(self._client.authenticate())
            self._authenticated = True

    def fetch_datasets_ids(self) -> List[str]:
        self._ensure_authenticated()

        query = """
        query ListDatasets($search: String, $limit: Int)  {
            datasets(first: $first, filters: { search: $search }) {
                if
            }
        }
        """
        data = self._runner.run(self._client.send(
            query, search=self.search, limit=self.limit))
        return [d["id"] for d in data["datasets"]]

    def fetch_dataset_croissant(self, dataset_id: str) -> requests.Response:
        self._ensure_authenticated()

        query = """
        query DatasetCroissant($id: ID!) {
            node(id: $id) {
                ... on Dataset {
                    croissantUrl
                }
            }
        }
        """

        data = self._runner.run(self._client.send(query, id=dataset_id))
        return data["dataset"]["croissantUrl"]


def main():
    AqoraHarvester.cli()


__all__ = ["AqoraHarvester"]
