import base64
import hashlib
import logging
import os
import random
from concurrent.futures import ThreadPoolExecutor
from http.client import RemoteDisconnected
from io import BytesIO
from urllib.parse import urlparse

import bencodepy
import pymongo
import urllib3
from minio import Minio
from requests import HTTPError
from tsing_spider.porn.jav import JAV_CATEGORIES, JAV_HOST, JAV_H_HOST, JAV_H_CATEGORIES, JavItem, BaseJavIndex

log = logging.getLogger(__file__)
item_thread_pool = ThreadPoolExecutor(max_workers=8)
mongodb_client = pymongo.MongoClient(os.environ["MONGODB_URI"])

collection = mongodb_client.get_database("resman").get_collection("spider_jav")
buffer_coll = mongodb_client.get_database("resman").get_collection(f"task_buffer_{random.randint(0, 100)}")

urllib3.disable_warnings()
s3_client = Minio(
    os.environ["S3_ENDPOINT"],
    access_key=os.environ["S3_ACCESS_KEY"],
    secret_key=os.environ["S3_SECRET_KEY"],
    secure=True,
    http_client=urllib3.PoolManager(
        timeout=urllib3.Timeout.DEFAULT_TIMEOUT,
        maxsize=16,
        cert_reqs='CERT_NONE',
        retries=urllib3.Retry(
            total=10,
            backoff_factor=0.2,
            status_forcelist=[500, 502, 503, 504]
        )
    )
)


def initialize():
    """
    Initialize mongodb and s3
    :return:
    """
    log.info("Initializing database")
    collection.create_index([("time", pymongo.DESCENDING)])
    collection.create_index([("published", pymongo.ASCENDING)])
    collection.create_index([("time", pymongo.ASCENDING)])
    buffer_coll.drop()
    if not s3_client.bucket_exists("jav"):
        s3_client.make_bucket("jav")


def finialize():
    buffer_coll.drop()


def generate_all_categories():
    for category in JAV_CATEGORIES:
        yield JAV_HOST, category
    for category in JAV_H_CATEGORIES:
        yield JAV_H_HOST, category


def create_magnet_uri(data: bytes):
    # noinspection PyTypeChecker
    metadata: dict = bencodepy.decode(data)
    subj = metadata[b'info']
    hashcontents = bencodepy.encode(subj)
    digest = hashlib.sha1(hashcontents).digest()
    b32hash = base64.b32encode(digest).decode()
    magnet_uri = 'magnet:?' + 'xt=urn:btih:' + b32hash
    if b"announce" in metadata:
        magnet_uri += ('&tr=' + metadata[b'announce'].decode())
    if b"info" in metadata:
        metadata_info = metadata[b'info']
        if b"name" in metadata_info:
            magnet_uri += ('&dn=' + metadata[b'info'][b'name'].decode())
        if b"length" in metadata_info:
            magnet_uri += ('&xl=' + str(metadata[b'info'][b'length']))
    return magnet_uri


def item_processor(item: JavItem):
    url_structure = [s for s in urlparse(item.url).path.split("/") if s != ""]
    category = url_structure[0]
    resource_id = url_structure[1]
    _id = f"{category}/{resource_id}"
    if collection.find_one({"_id": _id}) is None:
        log.info(f"Processing {item.url}.")
        try:
            with BytesIO(item.image) as fp:
                s3_client.put_object(
                    bucket_name="jav",
                    object_name=f"images/{_id}",
                    data=fp,
                    length=len(item.image),
                )
            magnet_uris = []
            for i, t in enumerate(item.torrents):
                magnet_uris.append(create_magnet_uri(t))
                with BytesIO(t) as fp:
                    s3_client.put_object(
                        bucket_name="jav",
                        object_name=f"torrents/{_id}/{i}.torrent",
                        data=fp,
                        length=len(t),
                        content_type="application/x-bittorrent"
                    )
            collection.insert_one(dict(
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
            ))
            log.info(f"{item.url} already processed successfully.")
        except RemoteDisconnected as _:
            log.warning(f"Server closed connection while downloading {item.url}, restarting...")
            item_thread_pool.submit(item_processor, JavItem(item.url))
        except Exception as ex:
            log.error(f"Failed to process {item.url}", exc_info=ex)


def item_tasks_generator(host, category, max_page_index: int = 5):
    page_index = 0
    errors_remain = 3
    while errors_remain > 0:
        page_index += 1
        if page_index > max_page_index:
            break
        base_page = BaseJavIndex(host, category, page_index)
        log.info(f"Reading {base_page.url}.")
        for i in range(5):
            try:
                for item in base_page.items:
                    if buffer_coll.find_one({"_id": item.url}) is None:
                        item_thread_pool.submit(item_processor, item)
                        buffer_coll.insert_one({"_id": item.url})
                break
            except HTTPError as he:
                if he.response.status_code == 404:
                    errors_remain -= 1
                else:
                    log.error(f"HTTP Error while reading {base_page.url}.", exc_info=he)
            except Exception as ex:
                log.error(f"Error while reading {base_page.url}.", exc_info=ex)


def start_task_pool():
    host_categories = list(generate_all_categories())
    base_thread_pool = ThreadPoolExecutor(max_workers=len(host_categories))
    for host, category in host_categories:
        base_thread_pool.submit(item_tasks_generator, host, category)
    base_thread_pool.shutdown(wait=True)
    item_thread_pool.shutdown(wait=True)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    initialize()
    start_task_pool()
    finialize()
