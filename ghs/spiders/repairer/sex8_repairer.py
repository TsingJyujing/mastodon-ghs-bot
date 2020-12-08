import logging
import mimetypes
from urllib.parse import urlparse

from bson import ObjectId
from tqdm import tqdm

from ghs.utils.storage import create_mongodb_client, create_s3_client, url_to_s3, bucket_name

mongodb_client = create_mongodb_client()
s3_client = create_s3_client()
collection = mongodb_client.get_database("resman").get_collection("spider_sex8")
log = logging.getLogger(__file__)


def remove_dir(dir_name: str):
    log.info(f"Removing dir {dir_name}")
    s3_client.remove_object()


def clean_sex8_by_intersection():
    # Get all list in MongoDB
    _id_set_mongo = {str(doc["_id"]) for doc in collection.find({}, {"_id": 1})}
    _id_set_s3 = {
        [s for s in obj.object_name.split("/") if s != ""][1]
        for obj in s3_client.list_objects(bucket_name, "sex8/")
    }
    remove_in_s3 = _id_set_s3 - _id_set_mongo
    remove_in_mongo = _id_set_mongo - _id_set_s3

    is_remove_s3 = input(
        f"Do you want to remove {len(remove_in_s3)} items in S3? "
        f"(Please input YES if you'd like to remove them)"
    ) == "YES" if remove_in_s3 else False

    is_remove_mongo = input(
        f"Do you want to remove {len(remove_in_mongo)} items in MongoDB? "
        f"(Please input YES if you'd like to remove them)"
    ) == "YES" if remove_in_mongo else False

    if is_remove_s3:
        log.info(f"Removing {len(remove_in_s3)} items in S3...")
        for _id in tqdm(remove_in_s3):
            objs = [x.object_name for x in s3_client.list_objects(bucket_name, prefix=f"/sex8/{_id}", recursive=True)]
            for obj in objs:
                s3_client.remove_object(bucket_name, obj)
            s3_client.remove_object(bucket_name, f"sex8/{_id}")

    if is_remove_mongo:
        log.info(f"Removing {len(remove_in_mongo)} items in MongoDB...")
        removed_count = collection.delete_many({"_id": {"$in": [
            ObjectId(_id) for _id in remove_in_mongo
        ]}}).deleted_count
        log.info(f"{removed_count}/{len(remove_in_mongo)} logs removed in MongoDB")


def repair_sex8_image():
    for doc in tqdm(collection.find(
            {"all_images_wrote": {"$ne": True}},
            {"subject": 1, "url": 1, "videos": 1, "_id": 1, "all_images_wrote": 1}
    ), total=collection.count({"all_images_wrote": {"$ne": True}})):
        _id = str(doc["_id"])
        file_list = {
            x.object_name for x in
            s3_client.list_objects(bucket_name, prefix=f"sex8/{_id}/", recursive=True)
        }
        completed_image_count = 0
        image_list = doc["subject"]["image_list"]
        for i, image_url in enumerate(image_list):
            url_path = urlparse(image_url).path
            mime_type = mimetypes.guess_type(url_path)[0]
            file_suffix = url_path.split(".")[-1]
            s3_path = f"sex8/{str(_id)}/images/{i}.{file_suffix}"
            if s3_path in file_list:
                completed_image_count += 1
            else:
                if url_to_s3(
                        s3_client,
                        image_url,
                        s3_path,
                        headers={"Referer": doc["url"]},
                        content_type=mime_type,
                        ignore_4xx=True
                ):
                    completed_image_count += 1
        if completed_image_count >= len(image_list):
            collection.update_one(
                {"_id": doc["_id"]},
                {"$set": {"all_images_wrote": True}}
            )
