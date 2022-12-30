from datetime import datetime
from os import path
from pbf_elaboration import elaborate_pbf
from pbf_download import download_pbf
import logging

#PBF_URL = "http://download.geofabrik.de/europe/italy/nord-est-221228.osm.pbf" # About 500MB
#PBF_URL = "http://download.geofabrik.de/europe/moldova-221228.osm.pbf" # About 62MB
#PBF_URL = "http://download.geofabrik.de/europe/luxembourg-221228.osm.pbf" # About 37MB
PBF_URL = "http://download.geofabrik.de/europe/andorra-221228.osm.pbf" # About 2MB

def main():
    logging.basicConfig(level=logging.INFO)
    logging.getLogger().addHandler(logging.FileHandler("logs.log"))
    logging.info("=====\t Start: %s \t=====", datetime.now().isoformat())

    pbf_path = path.basename(PBF_URL)

    download_pbf(PBF_URL, pbf_path)

    elaborate_pbf(pbf_path)

    logging.info("=====\t End: %s \t=====", datetime.now().isoformat())

if __name__ == '__main__':
    main()