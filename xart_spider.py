import logging
import mimetypes
import os
import random
from concurrent.futures import ThreadPoolExecutor
from http.client import RemoteDisconnected
from io import BytesIO
from threading import Thread
from urllib.parse import urlparse

import pymongo
import urllib3
from bson import ObjectId
from minio import Minio
from requests import HTTPError
from tsing_spider.porn.xarthunter import (
    XarthunterItemPage,
    XarthunterVideoIndexPage,
    XarthunterImageIndexPage
)
from tsing_spider.util import http_get

log = logging.getLogger(__file__)
item_thread_pool = ThreadPoolExecutor(max_workers=8)
mongodb_client = pymongo.MongoClient(os.environ["MONGODB_URI"])

collection = mongodb_client.get_database("resman").get_collection("spider_xart")
buffer_coll = mongodb_client.get_database("resman").get_collection(f"xart_task_buffer_{random.randint(0, 100)}")

urllib3.disable_warnings()
s3_bucket = "xart"
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
    collection.create_index([("published", pymongo.ASCENDING)])
    collection.create_index([("type", pymongo.ASCENDING)])
    collection.create_index([("url", pymongo.ASCENDING)])
    buffer_coll.drop()
    if not s3_client.bucket_exists(s3_bucket):
        s3_client.make_bucket(s3_bucket)


def finialize():
    """
    Do some clean up job after the spider finished
    :return:
    """
    buffer_coll.drop()


def item_processor(item: XarthunterItemPage):
    """
    Process item page
    :param item:
    :return:
    """
    if collection.find_one({"url": item.url}) is None:
        try:
            if item.is_video_page:
                video_item_processor(item)
            elif item.is_image_page:
                image_item_processor(item)
            else:
                log.warning("Can't find content in the page")
        except RemoteDisconnected as _:
            log.warning(f"Server closed connection while downloading {item.url}, restarting...")
            item_thread_pool.submit(image_item_processor, XarthunterItemPage(item.url))
        except Exception as ex:
            log.error(f"Failed to process {item.url}", exc_info=ex)


def image_item_processor(item: XarthunterItemPage):
    """
    Download all images to S3 storage and append item details to mongodb
    :param item:
    :return:
    """
    _id = ObjectId()
    doc = item.json
    doc["_id"] = _id
    s3_path_list = []
    for i, image_url in enumerate(item.image_urls):
        image_data = http_get(image_url, headers={"Referer": item.url})
        url_path = urlparse(image_url).path
        mime_type = mimetypes.guess_type(url_path)[0]
        file_suffix = url_path.split(".")[-1]
        s3_path = f"images/{str(_id)}/{i}.{file_suffix}"
        s3_path_list.append(s3_path)
        with BytesIO(image_data) as fp:
            s3_client.put_object(
                bucket_name=s3_bucket,
                object_name=s3_path,
                data=fp,
                length=len(image_data),
                content_type=mime_type
            )
    doc["url"] = item.url
    doc["type"] = "image"
    doc["published"] = False
    collection.insert_one(doc)
    log.info(f"Image {item.url} written.")


def video_item_processor(item: XarthunterItemPage):
    """
    Download video to S3 storage and append item details to mongodb
    :param item:
    :return:
    """
    _id = ObjectId()
    doc = item.json
    doc["_id"] = _id

    video_data = http_get(item.mp4_video_url, headers={"Referer": item.url})
    with BytesIO(video_data) as fp:
        s3_client.put_object(
            bucket_name=s3_bucket,
            object_name=f"videos/{str(_id)}/video.mp4",
            data=fp,
            length=len(video_data)
        )
    preview_image_data = http_get(item.preview_image_url, headers={"Referer": item.url})
    with BytesIO(preview_image_data) as fp:
        s3_client.put_object(
            bucket_name=s3_bucket,
            object_name=f"videos/{str(_id)}/preview.jpg",
            data=fp,
            length=len(preview_image_data),
            content_type="image/jpeg"
        )

    doc["url"] = item.url
    doc["type"] = "video"
    doc["published"] = False
    collection.insert_one(doc)
    log.info(f"Video {item.url} written.")


def item_tasks_generator(index_type: str, max_page_index: int):
    """
    Generate image/video items from the index page
    :param index_type: image/video
    :param max_page_index: Max pages to retrieval
    :return:
    """
    page_index = 0
    errors_remain = 3
    while errors_remain > 0:
        page_index += 1
        if page_index > max_page_index:
            break
        if index_type == "image":
            base_page = XarthunterImageIndexPage.create(page_index)
        elif index_type == "video":
            base_page = XarthunterVideoIndexPage.create(page_index)
        else:
            raise Exception(f"Unknown type: {index_type}")
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
    video_thread = Thread(target=item_tasks_generator, args=("video", 2,))
    video_thread.start()

    image_thread = Thread(target=item_tasks_generator, args=("image", 2,))
    image_thread.start()

    image_thread.join()
    video_thread.join()
    item_thread_pool.shutdown(wait=True)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    initialize()
    start_task_pool()
    finialize()
