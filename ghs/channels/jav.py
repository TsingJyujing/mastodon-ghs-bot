import random
from typing import Iterable
from urllib.parse import urlparse, parse_qs

import pymongo

from ghs.channels.base import BaseChannel, PushContent
from ghs.utils.storage import create_mongodb_client, create_s3_client

s3_client = create_s3_client()
mongodb_client = create_mongodb_client()
collection = mongodb_client.get_database("resman").get_collection("spider_jav")


def process_magnet_uri(magnet_uri: str):
    elements = []
    for k, vs in parse_qs(urlparse(magnet_uri).query).items():
        if k != "dn":
            for v in vs:
                elements.append(f"{k}={v}")
    return "magnet:?{}".format("&".join(elements))


class JAVChannel(BaseChannel):
    def __init__(self, category: str, category_display: str, candidate_count: int = 5):
        self.candidate_count = candidate_count
        self.category_display = category_display
        self.category = category

    def create_contents(self) -> Iterable[PushContent]:
        doc = random.choice(list(
            collection.find(
                {
                    "category": self.category,
                    "published": False
                }
            ).sort(
                "time", pymongo.DESCENDING
            ).limit(
                self.candidate_count
            )
        ))
        _id = doc["_id"]
        status = "《{}》\n\n{}\n\n磁力链：\n{}".format(
            doc["title"],
            " ".join("#{}".format(tag.replace(" ", "")) for tag in doc["tags"]),
            "\n".join(process_magnet_uri(uri) for uri in doc["magnet_uris"])
        )
        medias = [
            s3_client.get_object("jav", f"images/{_id}").data
        ]
        collection.update_one({"_id": _id}, update={"$set": {"published": True}})
        return [
            PushContent(
                title=self.category_display,
                text=status,
                medias=medias
            )
        ]
