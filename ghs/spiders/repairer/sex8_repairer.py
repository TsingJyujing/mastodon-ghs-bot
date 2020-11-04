import logging
import mimetypes
from tempfile import TemporaryFile
from urllib.parse import urlparse

import click
from bson import ObjectId
from tqdm import tqdm
from tsing_spider.util import M3U8Downloader

from ghs.utils.storage import create_mongodb_client, create_s3_client, url_to_s3

mongodb_client = create_mongodb_client()
s3_client = create_s3_client()
s3_bucket = "sex8"
collection = mongodb_client.get_database("resman").get_collection("spider_sex8")
log = logging.getLogger(__file__)


def remove_dir(dir_name: str):
    log.info(f"Removing dir {dir_name}")
    s3_client.remove_object()


def remove_by_intersection():
    # Get all list in MongoDB
    _id_set_mongo = {str(doc["_id"]) for doc in collection.find({}, {"_id": 1})}
    _id_set_s3 = {
        next(filter(lambda s: s != "", obj.object_name.split("/")))
        for obj in s3_client.list_objects_v2("sex8", start_after="0", )
    }
    remove_in_s3 = _id_set_s3 - _id_set_mongo
    remove_in_mongo = _id_set_mongo - _id_set_s3
    log.info(f"Removing {len(remove_in_s3)} items in S3...")
    for _id in tqdm(remove_in_s3):
        objs = [x.object_name for x in s3_client.list_objects("sex8", prefix=f"/{_id}", recursive=True)]
        for obj in objs:
            s3_client.remove_object("sex8", obj)
        s3_client.remove_object("sex8", f"{_id}")
    log.info(f"Removing {len(remove_in_mongo)} items in MongoDB...")
    removed_count = collection.delete_many({"_id": {"$in": [
        ObjectId(_id) for _id in remove_in_mongo
    ]}}).deleted_count
    log.info(f"{removed_count}/{len(remove_in_mongo)} logs removed in MongoDB")


@click.command()
@click.option("--re-download-images", default=True, help="Download images that haven't downloaded")
@click.option("--re-download-videos", default=True, help="Download videos that haven't downloaded")
def repair_sex8(
        re_download_images: bool,
        re_download_videos: bool
):
    remove_by_intersection()
    if re_download_images or re_download_videos:
        for doc in tqdm(collection.find({}, {"subject": 1, "url": 1, "videos": 1, "_id": 1})):
            _id = str(doc["_id"])
            file_list = {x.object_name for x in s3_client.list_objects("sex8", prefix=_id, recursive=True)}
            if re_download_images:
                for i, image_url in enumerate(doc["subject"]["image_list"]):
                    url_path = urlparse(image_url).path
                    mime_type = mimetypes.guess_type(url_path)[0]
                    file_suffix = url_path.split(".")[-1]
                    s3_path = f"{str(_id)}/images/{i}.{file_suffix}"
                    if s3_path not in file_list:
                        url_to_s3(
                            s3_client,
                            image_url,
                            s3_bucket,
                            s3_path,
                            headers={"Referer": doc["url"]},
                            content_type=mime_type,
                            ignore_4xx=True
                        )
                for i, video_url in enumerate(doc["videos"]):
                    s3_path = f"{str(_id)}/videos/{i}.ts"
                    if s3_path not in file_list:
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
