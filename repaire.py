import logging

import urllib3

from ghs.spiders.repairer.sex8_repairer import repair_sex8

if __name__ == '__main__':
    urllib3.disable_warnings()
    logging.basicConfig(level=logging.INFO)
    repair_sex8()
