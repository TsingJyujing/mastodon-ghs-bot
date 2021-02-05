import logging
from itertools import chain

import click
import urllib3

from ghs.spiders.base import run_generators
from ghs.spiders.jav import JavSpiderTaskGenerator
from ghs.spiders.sex8 import Sex8SpiderTaskGenerator, IMAGE_THREADS, VIDEO_THREADS, NOVEL_THREADS
from ghs.spiders.xart import XartSpiderTaskGenerator


@click.command()
@click.option("--sex8-image-pages", default=20, help="Page count of sex8cc images")
@click.option("--sex8-video-pages", default=5, help="Page count of sex8cc video")
@click.option("--sex8-novel-pages", default=10, help="Page count of sex8cc novel")
@click.option("--xart-pages", default=5, help="Page count of xart")
@click.option("--jav-pages", default=5, help="Page count of JAV")
def main(
        sex8_image_pages: int,
        sex8_video_pages: int,
        sex8_novel_pages: int,
        xart_pages: int,
        jav_pages: int,
):
    run_generators(
        list(chain(
            JavSpiderTaskGenerator.generate_all_generators(jav_pages),
            [Sex8SpiderTaskGenerator(fid, sex8_image_pages) for fid in IMAGE_THREADS],
            [Sex8SpiderTaskGenerator(fid, sex8_video_pages) for fid in VIDEO_THREADS],
            [Sex8SpiderTaskGenerator(fid, sex8_novel_pages) for fid in NOVEL_THREADS],
            [
                XartSpiderTaskGenerator("image", xart_pages),
                XartSpiderTaskGenerator("video", xart_pages),
            ]
        )),
        item_pool_workers=12
    )


if __name__ == '__main__':
    urllib3.disable_warnings()
    logging.basicConfig(level=logging.INFO)
    main()
