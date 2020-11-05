# GHS Mastodon Bot

GHS(搞黄色) is a Mastodon bot to publish porn media (image/video/game).

Now the robot is used by account [m.cmx.im/@nanoshinonome](https://m.cmx.im/@nanoshinonome/).
Please feel free to give your advice of it.

## Environment

We're using [poetry](https://python-poetry.org/docs/) to manage python environment & dependencies.

## Spider

### Introduction
Spider is used for collecting data from internet, it's mainly based on this project: [TsingJyujing/DataSpider](https://github.com/TsingJyujing/DataSpider).
The media will be saved to S3 compatible storage system like AWS S3/Minio/SeaweedFS/... and the meta data will be save to MongoDB.

### How to Start

```shell script
export MONGODB_URI="mongodb://xxx:yyy@x.x.x.x:27017/?authSource=xxx"
export S3_ENDPOINT=xxx.xxx.xxx
export S3_ACCESS_KEY=xxx
export S3_SECRET_KEY=xxx

poetry run python jav_spider.py
poetry run python xart_spider.py
```

## Bot

The bot is executed by crontab, it will push one random content after stared.

### How to Start

```shell script
export MONGODB_URI="mongodb://xxx:yyy@x.x.x.x:27017/?authSource=xxx"
export MASTODON_ID=xxxx
export MASTODON_SECRET=xxxx
export MASTODON_TOKEN=xxxx
export MASTODON_HOST=https://xxxx.xxxx.xxx
export S3_ENDPOINT=xxx.xxx.xxx
export S3_ACCESS_KEY=xxx
export S3_SECRET_KEY=xxx

poetry run python mastdon_robot.py
```

## TODOs

- Using celery to control publish schedule.
- Receiving request from other users
    - Maybe need SQL to support it.
- Collecting re-toot and star data for better recommendation
