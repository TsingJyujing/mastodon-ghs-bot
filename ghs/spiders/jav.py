import logging
from typing import Iterator, Callable
from urllib.parse import urlparse

import pymongo
import urllib3
from requests import HTTPError
from tsing_spider.porn.jav import JAV_CATEGORIES, JAV_HOST, JAV_H_HOST, JAV_H_CATEGORIES, JavItem, BaseJavIndex

from ghs.spiders.base import BaseSpiderTaskGenerator
from ghs.utils import create_magnet_uri
from ghs.utils.storage import create_s3_client, put_binary_data, create_mongodb_client, put_json

log = logging.getLogger(__file__)
urllib3.disable_warnings()

mongodb_client = create_mongodb_client()
s3_client = create_s3_client()


def initialize():
    """
    Initialize mongodb and s3
    :return:
    """
    log.info("Initializing database")
    collection = mongodb_client.get_database("resman").get_collection("spider_jav")
    collection.create_index([("time", pymongo.DESCENDING)])
    collection.create_index([("published", pymongo.ASCENDING)])
    collection.create_index([("time", pymongo.ASCENDING)])


def jav_item_processor(item: JavItem):
    def wrapper():
        collection = mongodb_client.get_database("resman").get_collection("spider_jav")
        url_structure = [s for s in urlparse(item.url).path.split("/") if s != ""]
        category = url_structure[0]
        resource_id = url_structure[1]
        _id = f"{category}/{resource_id}"
        if collection.find_one({"_id": _id}) is None:
            log.info(f"Processing {item.url}.")
            put_binary_data(s3_client, item.image, f"jav/images/{_id}")
            magnet_uris = []
            for i, torrent_binary_data in enumerate(item.torrents):
                magnet_uris.append(create_magnet_uri(torrent_binary_data))
                put_binary_data(
                    s3_client,
                    torrent_binary_data,
                    f"jav/torrents/{_id}/{i}.torrent",
                    content_type="application/x-bittorrent"
                )
            content_data = dict(
                _id=_id,
                category=category,
                resource_id=resource_id,
                title=item.title,
                image_url=item.image_url,
                magnet_uris=magnet_uris,
                torrent_resid_list=item.torrent_resid_list,
                tags=item.tags,
                time=item.time,
                published=False
            )
            put_json(s3_client, content_data, f"jav/torrents/{_id}/meta.json")
            collection.insert_one(content_data)
            log.info(f"{item.url} already processed successfully.")

    return wrapper


def generate_all_categories():
    for category in JAV_CATEGORIES:
        yield JAV_HOST, category
    for category in JAV_H_CATEGORIES:
        yield JAV_H_HOST, category


class JavSpiderTaskGenerator(BaseSpiderTaskGenerator):
    def __init__(self, host: str, category: str, max_page_index: int):
        self.max_page_index = max_page_index
        self.category = category
        self.host = host

    def generate(self) -> Iterator[Callable[[None], None]]:
        initialize()
        submitted_tasks = set()
        page_index = 0
        errors_remain = 3
        while errors_remain > 0:
            page_index += 1
            if page_index > self.max_page_index:
                break
            base_page = BaseJavIndex(self.host, self.category, page_index)
            log.info(f"Reading {base_page.url}.")
            for i in range(5):
                try:
                    for item in base_page.items:
                        if item.url not in submitted_tasks:
                            submitted_tasks.add(item.url)
                            yield jav_item_processor(item)
                    break
                except HTTPError as he:
                    if he.response.status_code == 404:
                        errors_remain -= 1
                    else:
                        log.error(f"HTTP Error while reading {base_page.url}.", exc_info=he)
                except Exception as ex:
                    log.error(f"Error while reading {base_page.url}.", exc_info=ex)

    @staticmethod
    def generate_all_generators(max_page_index: int):
        for h, c in generate_all_categories():
            yield JavSpiderTaskGenerator(h, c, max_page_index)
