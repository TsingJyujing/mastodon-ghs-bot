import random
from typing import Iterable

import pymongo

from ghs.channels.base import BaseChannel, PushContent
from ghs.utils.storage import create_mongodb_client, create_s3_client

mongodb_client = create_mongodb_client()
collection = mongodb_client.get_database("resman").get_collection("spider_sex8")
s3_client = create_s3_client()


class Sex8ImageChannel(BaseChannel):
    def __init__(self, forum_id: int, title: str, candidate_count: int = 20):
        self.title = title
        self.forum_id = forum_id
        self.candidate_count = candidate_count

    def create_contents(self) -> Iterable[PushContent]:
        doc = random.choice(list(
            collection.find(
                {
                    "forum_id": self.forum_id,
                    "published": False
                }
            ).sort(
                "_id", pymongo.DESCENDING
            ).limit(
                self.candidate_count
            )
        ))
        _id = doc["_id"]
        # Create Status and medias
        status = "{}".format(doc["title"])
        medias = [
            s3_client.get_object("sex8", obj.object_name).data
            for obj in s3_client.list_objects("sex8", f"{str(_id)}/images/")
        ]
        collection.update_one({"_id": _id}, update={"$set": {"published": True}})
        batches = len(medias) // 4 + 1
        return [
            PushContent(
                title=f"{self.title} {i + 1}/{batches}",
                text=status,
                medias=medias[i * 4:(i + 1) * 4]
            )
            for i in range(batches)
        ]
