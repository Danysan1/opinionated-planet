from urllib import request
from datetime import datetime
from os import path
import progressbar
import logging

class MyProgressBar():
    def __init__(self):
        self.pbar = None

    def __call__(self, block_num, block_size, total_size):
        if not self.pbar:
            self.pbar=progressbar.ProgressBar(maxval=total_size)
            self.pbar.start()

        downloaded = block_num * block_size
        if downloaded < total_size:
            self.pbar.update(downloaded)
        else:
            self.pbar.finish()

def download_pbf(pbf_url:str, pbf_path:str):
    if not path.exists(pbf_path):
        try:
            logging.info("Download started at %s", datetime.now().isoformat())
            request.urlretrieve(pbf_url, pbf_path, MyProgressBar())
        except BaseException as err:
            logging.error("Download failed at %s", datetime.now().isoformat())
            raise err
        logging.info("Download finished at %s", datetime.now().isoformat())