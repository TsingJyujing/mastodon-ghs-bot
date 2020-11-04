import logging
from abc import ABC, abstractmethod
from concurrent.futures.thread import ThreadPoolExecutor
from typing import Callable, Iterator, List

log = logging.getLogger(__file__)


class BaseSpiderTaskGenerator(ABC):
    @abstractmethod
    def generate(self) -> Iterator[Callable[[None], None]]: pass


def run_generators(task_generators: List[BaseSpiderTaskGenerator], item_pool_workers: int = 32, retries: int = 5):
    g_pool = ThreadPoolExecutor(max_workers=len(task_generators))
    i_pool = ThreadPoolExecutor(max_workers=item_pool_workers)

    def retry_and_handle_exception(func: Callable):
        def wrapper():
            for i in range(retries):
                try:
                    func()
                    break
                except Exception as ex:
                    log.error(f"Error while executing task (retries={i}).", exc_info=ex)

        return wrapper

    def submit_all_items(stg: BaseSpiderTaskGenerator):
        def wrapper():
            try:
                for sub_task in stg.generate():
                    i_pool.submit(retry_and_handle_exception(sub_task))
            except Exception as ex:
                log.error("Error while generating tasks.", exc_info=ex)

        return wrapper

    for g in task_generators:
        g_pool.submit(submit_all_items(g))
    log.info("All generators started.")
    g_pool.shutdown(wait=True)
    log.info("All generators terminated.")
    i_pool.shutdown(wait=True)
    log.info("All tasks finished.")
