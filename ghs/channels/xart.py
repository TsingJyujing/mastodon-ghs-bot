import random
from math import ceil
from typing import Iterable
from urllib.parse import urlparse, parse_qs

import pymongo

from ghs.channels.base import BaseChannel, PushContent
from ghs.utils.storage import create_s3_client, create_mongodb_client, bucket_name

mongodb_client = create_mongodb_client()
collection = mongodb_client.get_database("resman").get_collection("spider_xart")
s3_client = create_s3_client()


def process_magnet_uri(magnet_uri: str):
    elements = []
    for k, vs in parse_qs(urlparse(magnet_uri).query).items():
        if k != "dn":
            for v in vs:
                elements.append(f"{k}={v}")
    return "magnet:?{}".format("&".join(elements))


class XartImageChannel(BaseChannel):
    def __init__(self, candidate_count: int = 20):
        self.candidate_count = candidate_count

    def create_contents(self) -> Iterable[PushContent]:
        doc = random.choice(list(
            collection.find(
                {
                    "type": "image",
                    "published": False
                }
            ).sort(
                "like_count", pymongo.DESCENDING
            ).limit(
                self.candidate_count
            )
        ))
        _id = doc["_id"]
        status = "《{}》".format(doc["title"])
        medias = [
            s3_client.get_object(bucket_name, obj.object_name).data
            for obj in s3_client.list_objects(bucket_name, f"xart/images/{str(_id)}/")
        ]
        collection.update_one({"_id": _id}, update={"$set": {"published": True}})

        batches = ceil(len(medias) / 4.0)
        return [
            PushContent(
                title="Xart图片集",
                text=status,
                medias=medias[i * 4:(i + 1) * 4]
            )
            for i in range(batches)
        ]


class XartVideoChannel(BaseChannel):
    def __init__(self, candidate_count: int = 20):
        self.candidate_count = candidate_count

    def create_contents(self) -> Iterable[PushContent]:
        doc = random.choice(list(
            collection.find(
                {
                    "type": "video",
                    "published": False
                }
            ).sort(
                "like_count", pymongo.DESCENDING
            ).limit(
                self.candidate_count
            )
        ))
        _id = doc["_id"]
        status = "《{}》".format(
            doc["title"],
        )
        medias = [
            s3_client.get_object(bucket_name, f"xart/videos/{str(_id)}/video.mp4").data
        ]
        collection.update_one({"_id": _id}, update={"$set": {"published": True}})
        return [
            PushContent(
                title="Xart视频预览",
                text=status,
                medias=medias
            )
        ]
