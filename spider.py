import logging

import urllib3

from ghs.spiders.base import run_generators
from ghs.spiders.jav import JavSpiderTaskGenerator
from ghs.spiders.sex8 import Sex8SpiderTaskGenerator
from ghs.spiders.xart import XartSpiderTaskGenerator

SEX8CC_PAGE_COUNT = {
    # IMAGES
    157: 15,  # 生活自拍
    158: 15,  # 性爱自拍
    11: 15,   # 亚洲图区
    # # NOVELS
    # 279: 15,  # 253,
    # 858: 15,  # 556,
    # # VIDEOS
    # 904: 15,  # 440
    # 181: 15,  # 4081
}

if __name__ == '__main__':
    urllib3.disable_warnings()
    logging.basicConfig(level=logging.INFO)
    max_page_index = 5
    generator_list = list(JavSpiderTaskGenerator.generate_all_generators(max_page_index))
    generator_list.append(XartSpiderTaskGenerator("image", max_page_index))
    generator_list.append(XartSpiderTaskGenerator("video", max_page_index))
    for fid, mpi in SEX8CC_PAGE_COUNT.items():
        generator_list.append(Sex8SpiderTaskGenerator(fid, mpi))
    run_generators(generator_list, item_pool_workers=12)
