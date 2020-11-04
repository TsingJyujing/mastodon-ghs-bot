import logging
import os
from io import BytesIO

import pymongo
import urllib3
from minio import Minio
from requests import HTTPError
from tsing_spider.util import http_get

log = logging.getLogger(__file__)


def create_mongodb_client():
    """
    Create mongodb client instance
    :return:
    """
    return pymongo.MongoClient(os.environ["MONGODB_URI"])


def create_s3_client(secure: bool = True, ignore_cert: bool = True):
    """
    Create S3 storage client instance
    :param secure:
    :param ignore_cert:
    :return:
    """
    return Minio(
        os.environ["S3_ENDPOINT"],
        access_key=os.environ["S3_ACCESS_KEY"],
        secret_key=os.environ["S3_SECRET_KEY"],
        secure=secure,
        http_client=urllib3.PoolManager(
            timeout=urllib3.Timeout.DEFAULT_TIMEOUT,
            maxsize=128,
            cert_reqs='CERT_NONE',
            retries=urllib3.Retry(
                total=10,
                backoff_factor=0.2,
                status_forcelist=[500, 502, 503, 504]
            )
        ) if secure and ignore_cert else None
    )


def put_binary_data(s3_client: Minio, data: bytes, bucket_name: str, object_name: str, **kwargs):
    with BytesIO(data) as fp:
        return s3_client.put_object(
            bucket_name=bucket_name,
            object_name=object_name,
            data=fp,
            length=len(data),
            **kwargs
        )


def url_to_s3(
        s3_client: Minio,
        url: str,
        bucket_name: str,
        object_name: str,
        headers: dict = None,
        content_type='application/octet-stream',
        ignore_4xx: bool = True
):
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
    except HTTPError as he:
        status_code = he.response.status_code
        if ignore_4xx and 400 <= status_code < 500:
            log.warning(f"Can't download data from {url} since resource is not able to access ({status_code}).")
        else:
            raise he
