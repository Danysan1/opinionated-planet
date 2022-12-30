import logging
import re
import json
from datetime import datetime
from os import path
import pandas as pd
import osmium
from qwikidata.sparql import return_sparql_query_results
from time import sleep

WIKIDATA_Q_ID_REGEX = r"^Q\d+$"
WIKIDATA_Q_IDS_REGEX = r"^Q\d+(?:;Q\d+)*$"

class WikidataIdHandler(osmium.SimpleHandler):
    def __init__(self):
        super(WikidataIdHandler, self).__init__()
        self.wd_ids = set()
        self.count = 0

    def check(self, tags:osmium.osm.TagList):
        self.count+=1
        if self.count % 1_000_000 == 0:
            logging.info("Analysed elements: %d", self.count)

        if "wikidata" in tags:
            qid = tags.get("wikidata").partition(";")[0]
            if re.match(WIKIDATA_Q_ID_REGEX, qid):
                self.wd_ids.add(qid)
            else:
                logging.info("Skipping bad Q-ID %s", qid)

    def node(self, n):
        self.check(n.tags)

    def way(self, w):
        self.check(w.tags)

    def relation(self, r):
        self.check(r.tags)

def get_wikidata_ids(pbf_path:str, skip_cache:bool=False) -> set:
    file_path = f"{pbf_path}.wikidata_ids.csv"
    if skip_cache or not path.exists(file_path):
        handler = WikidataIdHandler()
        logging.info("Pre Wikidata ID search: %s", datetime.now().isoformat())
        handler.apply_file(pbf_path)
        logging.info("Post Wikidata ID search: %s", datetime.now().isoformat())
        ret = handler.wd_ids
        with open(file_path,'w') as f:
            f.write('\n'.join(ret))
    else:
        ret = set(line.strip() for line in open(file_path,'r'))      

    logging.info("Found %d IDs", len(ret))
    return ret

def buildSparqlLabelQuery(wikidata_ids) -> str:
    ids_string = " ".join(map(lambda id: f"wd:{id}", wikidata_ids))
    return f"""
        SELECT ?id ?lang ?label
        WHERE {{
            VALUES ?id {{ {ids_string} }}
            ?id rdfs:label ?labelObj.
            BIND(LANG(?labelObj) AS ?lang).
            FILTER( STRLEN(?lang) < 3 ).
            BIND(CONCAT(UCASE(SUBSTR(STR(?labelObj), 1, 1)), SUBSTR(STR(?labelObj), 2)) AS ?label) # Title case
        }}
    """

WDQS_MAX_SIZE = 300

def fetch_wikidata_labels_df(wikidata_ids:set) -> pd.DataFrame:
    labels = []
    wikidata_ids_list = list(wikidata_ids)

    logging.info("Pre Wikidata SPARQL query: %s", datetime.now().isoformat())
    for i in range(0, len(wikidata_ids_list), WDQS_MAX_SIZE): # Paging
        if i % (WDQS_MAX_SIZE*10):
            logging.info("Downloaded labels for %d entities", i)
        query = buildSparqlLabelQuery(wikidata_ids_list[i:i+WDQS_MAX_SIZE])
        # with open("wikidata.rq", "w") as file:
        #     file.write(query)
        res = return_sparql_query_results(query)
        labels.extend([
                x["id"]["value"].replace("http://www.wikidata.org/entity/",""),
                x["lang"]["value"],
                x["label"]["value"]
            ] for x in res["results"]["bindings"])
        sleep(0.4)
    logging.info("Post Wikidata SPARQL query: %s", datetime.now().isoformat())
    
    df = pd.DataFrame(labels, columns=["id","lang","label"])
    df["key"] = "name:"+df["lang"]
    return df

def get_wikidata_labels_df(pbf_path:str, skip_cache:bool=False):
    ids = get_wikidata_ids(pbf_path, skip_cache)
    csv_path = f"{pbf_path}.wikidata_labels.csv"
    if skip_cache or not path.exists(csv_path):
        df = fetch_wikidata_labels_df(ids)
        df.to_csv(csv_path)
    else:
        df = pd.read_csv(csv_path)
    
    logging.info("Wikidata labels DataFrame:\n%s", df.describe(include = 'all'))
    return df
