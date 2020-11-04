import logging
import mimetypes
from concurrent.futures import ThreadPoolExecutor
from http.client import RemoteDisconnected
from io import BytesIO
from typing import Iterator, Callable
from urllib.parse import urlparse

import pymongo
import urllib3
from bson import ObjectId
from requests import HTTPError
from tsing_spider.porn.xarthunter import (
    XarthunterItemPage,
    XarthunterVideoIndexPage,
    XarthunterImageIndexPage
)
from tsing_spider.util import http_get

from ghs.spiders.base import BaseSpiderTaskGenerator
from ghs.utils.storage import create_s3_client, create_mongodb_client

log = logging.getLogger(__file__)
item_thread_pool = ThreadPoolExecutor(max_workers=8)
mongodb_client = create_mongodb_client()

collection = mongodb_client.get_database("resman").get_collection("spider_xart")

urllib3.disable_warnings()
s3_bucket = "xart"
s3_client = create_s3_client()


def initialize():
    """
    Initialize mongodb and s3
    :return:
    """
    log.info("Initializing database")
    collection.create_index([("published", pymongo.ASCENDING)])
    collection.create_index([("type", pymongo.ASCENDING)])
    collection.create_index([("url", pymongo.ASCENDING)])
    if not s3_client.bucket_exists(s3_bucket):
        s3_client.make_bucket(s3_bucket)


def get_item_processor(item: XarthunterItemPage):
    def item_processor():
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

    return item_processor


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
        try:
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
        except HTTPError as he:
            if 400 <= he.response.status_code < 500:
                log.warning(f"Can't download image {image_url} since resource is not able to access (4xx).")
            else:
                raise he

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
    if item.preview_image_url is not None:
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


class XartSpiderTaskGenerator(BaseSpiderTaskGenerator):
    def __init__(self, index_type: str, max_page_index: int):
        self.max_page_index = max_page_index
        self.index_type = index_type

    def generate(self) -> Iterator[Callable[[None], None]]:
        initialize()
        page_index = 0
        errors_remain = 3
        submitted_tasks = set()
        while errors_remain > 0:
            page_index += 1
            if page_index > self.max_page_index:
                break
            if self.index_type == "image":
                base_page = XarthunterImageIndexPage.create(page_index)
            elif self.index_type == "video":
                base_page = XarthunterVideoIndexPage.create(page_index)
            else:
                raise Exception(f"Unknown type: {self.index_type}")

            for i in range(5):
                try:
                    for item in base_page.items:
                        if item.url not in submitted_tasks:
                            submitted_tasks.add(item.url)
                            yield get_item_processor(item)
                    break
                except HTTPError as he:
                    if he.response.status_code == 404:
                        errors_remain -= 1
                    else:
                        log.error(f"HTTP Error while reading {base_page.url}.", exc_info=he)
                except Exception as ex:
                    log.error(f"Error while reading {base_page.url}.", exc_info=ex)
