import logging
from itertools import chain

import urllib3

from ghs.spiders.base import run_generators
from ghs.spiders.jav import JavSpiderTaskGenerator
from ghs.spiders.sex8 import Sex8SpiderTaskGenerator
from ghs.spiders.xart import XartSpiderTaskGenerator
import click


@click.command()
@click.option("--sex8-pages", default=20, help="Page count of sex8cc")
@click.option("--xart-pages", default=5, help="Page count of xart")
@click.option("--jav-pages", default=5, help="Page count of JAV")
def main(
        sex8_pages: int,
        xart_pages: int,
        jav_pages: int,
):
    run_generators(
        list(chain(
            JavSpiderTaskGenerator.generate_all_generators(jav_pages),
            (
                Sex8SpiderTaskGenerator(fid, mpi)
                for fid, mpi in {
                # IMAGES
                157: sex8_pages,  # 生活自拍
                158: sex8_pages,  # 性爱自拍
                11: sex8_pages,  # 亚洲图区
                # # NOVELS
                # 279: 15,  # 253,
                # 858: 15,  # 556,
                # # VIDEOS
                # 904: 15,  # 440
                # 181: 15,  # 4081
            }.items()
            ), [
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
