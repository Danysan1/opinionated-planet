from urllib import request
from datetime import datetime
from os import path
from pbf_elaboration import elaborate_pbf
import progressbar

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

print("Start: ", datetime.now().isoformat())

#pbf_url = "http://download.geofabrik.de/europe/italy/nord-est-221228.osm.pbf" # About 500MB
#pbf_url = "http://download.geofabrik.de/europe/moldova-221228.osm.pbf" # About 62MB
#pbf_url = "http://download.geofabrik.de/europe/luxembourg-221228.osm.pbf" # About 37MB
pbf_url = "http://download.geofabrik.de/europe/andorra-221228.osm.pbf" # About 2MB

pbf_path = path.basename(pbf_url)

if not path.exists(pbf_path):
    try:
        print("Download started at", datetime.now().isoformat())
        request.urlretrieve(pbf_url, pbf_path, MyProgressBar())
    except Exception as err:
        print("Download failed at", datetime.now().isoformat())
        raise err
    print("Download finished at", datetime.now().isoformat())

elaborate_pbf(pbf_path)