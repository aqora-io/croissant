import asyncio
import inspect
from typing import Any, Callable, List

import requests
from aqora_cli import Client

from croissant_rdf._src.croissant_harvester import CroissantHarvester

__author__ = "Angel Dijoux"


def _run_async(fn: Callable[..., Any], *args, **kwargs):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    async def runner():
        result = fn(*args, **kwargs)
        if inspect.isawaitable(result):
            return await result
        return result

    try:
        return loop.run_until_complete(runner())
    finally:
        loop.close()


class AqoraHarvester(CroissantHarvester):
    api_url = "https://aqora.io"

    def __init__(self):
        super().__init__()
        self._client = Client()
        self._authenticated = False

    def _ensure_authenticated(self):
        if not self._authenticated:
            _run_async(self._client.authenticate)
            self._authenticated = True

    def fetch_datasets_ids(self) -> List[str]:
        self._ensure_authenticated()

        query = """
            query ListDatasets($search: String, $limit: Int)  {
                datasets(first: $limit, filters: { search: $search }) {
                    nodes {
                        id
                    }
                }
            }
        """
        data = _run_async(self._client.send, query,
                          search=self.search, limit=self.limit)
        return [d["id"] for d in data["datasets"]["nodes"]]

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

        data = _run_async(self._client.send, query, id=dataset_id)
        return data["node"]["croissantUrl"]


def main():
    AqoraHarvester.cli()


__all__ = ["AqoraHarvester"]
