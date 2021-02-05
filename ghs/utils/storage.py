import logging
import os
from io import BytesIO
from urllib.parse import urlparse, unquote_plus

import pymongo
from bson.json_util import dumps
from minio import Minio
from requests import HTTPError
from tsing_spider.util import http_get

log = logging.getLogger(__file__)

bucket_name: str = os.environ.get("S3_BUCKET", "spider")


def create_mongodb_client():
    """
    Create mongodb client instance
    :return:
    """
    return pymongo.MongoClient(os.environ["MONGODB_URI"])


def create_s3_client():
    """
    Create S3 storage client instance
    :environment variable: S3_URI: e.x. https://access_key:secret_key@s3.xxx.com/
    :return:
    """
    parse_result = urlparse(os.environ["S3_URI"])
    mc = Minio(
        parse_result.hostname,
        access_key=unquote_plus(parse_result.username),
        secret_key=unquote_plus(parse_result.password),
        secure=parse_result.scheme == "https"
    )
    if not mc.bucket_exists(bucket_name):
        mc.make_bucket(bucket_name)
    return mc


def put_binary_data(s3_client: Minio, data: bytes, object_name: str, **kwargs):
    with BytesIO(data) as fp:
        return s3_client.put_object(
            bucket_name=bucket_name,
            object_name=object_name,
            data=fp,
            length=len(data),
            **kwargs
        )


def put_json(s3_client: Minio, data_object, object_name: str, **kwargs):
    return put_binary_data(
        s3_client,
        dumps(data_object).encode(),
        object_name,
        content_type="application/json",
        **kwargs
    )


def url_to_s3(
        s3_client: Minio,
        url: str,
        object_name: str,
        headers: dict = None,
        content_type='application/octet-stream',
        ignore_4xx: bool = True
) -> bool:
    try:
        bin_data = http_get(url, headers=headers)
        with BytesIO(bin_data) as fp:
            s3_client.put_object(
                bucket_name=bucket_name,
                object_name=object_name,
                data=fp,
                length=len(bin_data),
                content_type=content_type
            )
        return True
    except HTTPError as he:
        status_code = he.response.status_code
        if ignore_4xx and 400 <= status_code < 500:
            log.warning(f"Can't download data from {url} since resource is not able to access ({status_code}).")
        else:
            raise he
    return False
