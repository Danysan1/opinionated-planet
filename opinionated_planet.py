from urllib import request
from datetime import datetime
from os import path
from pbf_elaboration import elaborate_pbf


print("Start: ", datetime.now().isoformat())

#pbf_url = "http://download.geofabrik.de/europe/italy/nord-est-221226.osm.pbf" # About 500MB
pbf_url = "http://download.geofabrik.de/europe/moldova-221226.osm.pbf" # About 62MB
#pbf_url = "http://download.geofabrik.de/europe/luxembourg-221226.osm.pbf" # About 37MB
pbf_path = path.basename(pbf_url)

if not path.exists(pbf_path):
    request.urlretrieve(pbf_url, pbf_path)

elaborate_pbf(pbf_path, True)