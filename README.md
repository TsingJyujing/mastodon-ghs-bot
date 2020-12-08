# GHS Mastodon Bot

GHS(搞黄色) is a Mastodon bot to publish porn media (image/video/game).

Now the robot is used by account [m.cmx.im/@nanoshinonome](https://m.cmx.im/@nanoshinonome/).
Please feel free to give your advices.

## Environment

We're using [poetry](https://python-poetry.org/docs/) to manage python environment & dependencies.

But the packages already exported to [requirements.txt](requirements.txt), you can install environment by:

```shell script
pip3 install -r requirements.txt
```

We also provided docker container for environment, please run
```shell script
sudo docker build -t tsingjyujing/ghs-mastodon-bot .
```
to build environment or just pull the image directly.

## Spider

### Introduction
Spider is used for collecting data from internet, it mainly base on this project: [TsingJyujing/DataSpider](https://github.com/TsingJyujing/DataSpider).
The media will be saved to S3 compatible storage system like AWS S3/Minio/SeaweedFS/... and the meta data will be save to MongoDB.

### How to Start

```shell script
export MONGODB_URI="mongodb://xxx:yyy@x.x.x.x:27017/?authSource=xxx"
export S3_URI=https://<S3_ACCESS_KEY>:<S3_SECRET_KEY>@S3_ENDPOINT/
export S3_BUCKET=<S3 bucket name, default is "spider">

poetry run python spider.py
```

Or you can start by docker:

```shell script
docker run -it \
  -e MONGODB_URI=mongodb://xxxx:xxx@mongodb.xxx.com:27017/?authSource=xxxx \
  -e S3_URI=https://xxxxx:xxx@s3.xxx.com/ \
  -e S3_BUCKET=xxxx \
  tsingjyujing/ghs-mastodon-bot python3 spider.py
```


## Bot

The bot is executed by crontab, it will push one random content after stared.

### How to Start

```shell script
export MONGODB_URI="mongodb://xxx:yyy@x.x.x.x:27017/?authSource=xxx"
export MASTODON_TOKEN=xxxx
export MASTODON_HOST=https://xxxx.xxxx.xxx
export S3_URI=https://<S3_ACCESS_KEY>:<S3_SECRET_KEY>@S3_ENDPOINT/
export S3_BUCKET=<S3 bucket name, default is "spider">

poetry run python mastodon_robot.py
```

Or you can start by docker:

```shell script
sudo docker run -it \
  -e MONGODB_URI=mongodb://xxxx:xxx@mongodb.xxx.com:27017/?authSource=xxxx \
  -e S3_URI=https://xxxxx:xxx@s3.xxx.com/ \
  -e S3_BUCKET=xxxx \
  -e MASTODON_TOKEN=xx-xxxxxx \
  -e MASTODON_HOST=https://x.xxx.xx \
  tsingjyujing/ghs-mastodon-bot python3 mastodon_robot.py
```

## TODOs

- Using celery to control publish schedule.
- Receiving request from other users
    - Maybe need SQL to support it.
- Collecting re-toot and star data for better recommendation
- Video Processing of ts files: [kkroening/ffmpeg-python](https://github.com/kkroening/ffmpeg-python)