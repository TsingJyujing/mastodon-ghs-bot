import logging
import mimetypes
from tempfile import TemporaryFile
from typing import Iterator, Callable
from urllib.parse import urlparse

import pymongo
from bson import ObjectId
from requests import HTTPError
from tsing_spider.porn.sex8cc import ForumPage, ForumThread
from tsing_spider.util.hls import M3U8Downloader

from ghs.spiders.base import BaseSpiderTaskGenerator
from ghs.utils.storage import create_s3_client, url_to_s3, create_mongodb_client

log = logging.getLogger(__file__)

mongodb_client = create_mongodb_client()
s3_client = create_s3_client()
s3_bucket = "sex8"
collection = mongodb_client.get_database("resman").get_collection("spider_sex8")


def initialize():
    """
    Initialize mongodb and s3
    :return:
    """
    log.info("Initializing database")
    collection.create_index([("published", pymongo.ASCENDING)])
    collection.create_index([("url", pymongo.ASCENDING)])
    collection.create_index([("forum_id", pymongo.ASCENDING)])
    if not s3_client.bucket_exists(s3_bucket):
        s3_client.make_bucket(s3_bucket)


def thread_item_processor(forum_thread: ForumThread, forum_id: int):
    def wrapper():
        if collection.find_one({"url": forum_thread.url}) is None:
            _id = ObjectId()
            data = forum_thread.json
            data["_id"] = _id
            data["forum_id"] = forum_id
            data["published"] = False

            for i, image_url in enumerate(forum_thread.subject.image_list):
                log.debug(f"Downloading image {i} for page {forum_thread.url}")
                url_path = urlparse(image_url).path
                mime_type = mimetypes.guess_type(url_path)[0]
                file_suffix = url_path.split(".")[-1]
                s3_path = f"{str(_id)}/images/{i}.{file_suffix}"
                url_to_s3(
                    s3_client,
                    image_url,
                    s3_bucket,
                    s3_path,
                    headers={"Referer": forum_thread.url},
                    content_type=mime_type,
                    ignore_4xx=True
                )
            for i, video_url in enumerate(forum_thread.m3u8_video_links):
                log.info(f"Downloading video {i} for page {forum_thread.url}")
                s3_path = f"{str(_id)}/videos/{i}.ts"
                with TemporaryFile() as fp:
                    total_size = 0
                    for bs in M3U8Downloader(video_url).data_stream():
                        total_size += len(bs)
                        fp.write(bs)
                    fp.seek(0)
                    s3_client.put_object(
                        bucket_name=s3_bucket,
                        object_name=s3_path,
                        data=fp,
                        length=total_size,
                        content_type="video/MP2T",
                    )
            collection.insert_one(data)
            log.info(f"{forum_thread.url} already processed successfully.")

    return wrapper


class Sex8SpiderTaskGenerator(BaseSpiderTaskGenerator):
    def __init__(self, forum_id: int, max_page_index: int):
        self.max_page_index = max_page_index
        self.forum_id = forum_id

    def generate(self) -> Iterator[Callable[[None], None]]:
        submitted_tasks = set()
        page_index = 0
        errors_remain = 3
        while errors_remain > 0:
            page_index += 1
            if page_index > self.max_page_index:
                break

            base_page = ForumPage(self.forum_id, page_id=page_index)
            log.info(f"Reading {base_page.url}.")
            for i in range(5):
                try:
                    for thread_item in base_page.thread_list:
                        if thread_item.url not in submitted_tasks:
                            submitted_tasks.add(thread_item.url)
                            yield thread_item_processor(thread_item, self.forum_id)
                    break
                except HTTPError as he:
                    if he.response.status_code == 404:
                        errors_remain -= 1
                    else:
                        log.error(f"HTTP Error while reading {base_page.url}.", exc_info=he)
                except Exception as ex:
                    log.error(f"Error while reading {base_page.url}.", exc_info=ex)
