import json
import os
from mimetypes import guess_type
from urllib.parse import urlparse, unquote_plus

from bson.json_util import dumps
from resman_client.client import ResmanClient, DefaultS3Image, ImageList, VideoList
from tqdm import tqdm

from ghs.spiders.sex8 import IMAGE_THREADS, VIDEO_THREADS
from ghs.utils.storage import create_mongodb_client, create_s3_client


def filename_to_index(filename: str):
    return int(filename.split("/")[-1].split(".")[0])


def create_resman_client():
    u = urlparse(os.environ["RESMAN_URI"])
    return ResmanClient(
        f"{u.scheme}://{u.hostname}/",
        unquote_plus(u.username),
        unquote_plus(u.password)
    )


if __name__ == '__main__':
    mongo = create_mongodb_client()
    mc = create_s3_client()
    ic = create_resman_client()
    coll = mongo.get_database("resman").get_collection("spider_sex8")
    for doc in tqdm(list(coll.find({
        "migrated": {"$ne": True},
        "forum_id": {"$in": IMAGE_THREADS}
    })), desc="Migrating images"):
        _id = doc["_id"]
        image_list = []
        for obj in mc.list_objects("spider", f"sex8/{str(_id)}/images/"):
            content_type = guess_type(obj.object_name)[0]
            if content_type is not None and content_type.startswith("image"):
                image_list.append(DefaultS3Image(
                    bucket="spider",
                    object_name=obj.object_name,
                    order=filename_to_index(obj.object_name)
                ))
        if len(image_list) > 0:
            ic.create_image_list(
                ImageList(
                    title=doc["title"],
                    description=doc["subject"]["content"],
                    data=json.loads(dumps(doc))
                )
            ).append_s3_images(image_list)
        coll.update_one({"_id": _id}, {"$set": {"migrated": True}})

    for doc in tqdm(list(coll.find({
        "migrated": {"$ne": True},
        "forum_id": {"$in": VIDEO_THREADS}
    })), desc="Migrating videos"):
        _id = doc["_id"]
        s3_video_list = [
            obj.object_name
            for obj in mc.list_objects("spider", f"sex8/{str(_id)}/videos/")
        ]

        if len(s3_video_list) > 0:
            video_list = ic.create_video_list(VideoList(
                title=doc["title"],
                description=doc["subject"]["content"],
                data=json.loads(dumps(doc)),
            ))
            for s3_video in s3_video_list:
                video_list.append_s3_video("spider", s3_video)

        coll.update_one({"_id": _id}, {"$set": {"migrated": True}})
