import logging

import urllib3

from ghs.spiders.repairer.sex8_repairer import (
    repair_sex8_image,
    clean_sex8_by_intersection
)

if __name__ == '__main__':
    urllib3.disable_warnings()
    logging.basicConfig(level=logging.INFO)
    clean_sex8_by_intersection()
    repair_sex8_image()
