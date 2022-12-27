from urllib import request
import re
import json
from datetime import datetime
from os import path
import pandas as pd
import osmium

def download_deprecated_wiki(wiki_file_name:str):
    url = "https://wiki.openstreetmap.org/w/api.php?action=query&format=json&titles=Template%3ADeprecated_features&redirects=0&prop=revisions&rvprop=content&indexpageids=1"
    with request.urlopen(url) as json_response:
        response = json.load(json_response)
    pageID = response["query"]["pageids"][0]
    csv = response["query"]["pages"][pageID]["revisions"][0]["*"]
    with open(wiki_file_name, "w") as wiki_file:
        wiki_file.write(csv)

CARRY_VALUE = "__CARRY__"

DEPRECATED_FEATURES_REGEX = [[
    lambda x: [x[0],x[1],x[2],x[3],x[4],x[5],x[6],x[7],"dkey_dvalue_fixed_fixed"],
    r'''\{\{Deprecated features/item\|lang=\{\{\{lang\|\}\}\}\|date=([\d-]*).*
\|dkey=([:_\w]+)\|dvalue ?= ?([_\w]+)\|?.*
\|suggestion=\{\{(?:key|tag)\|([:_\w]+)\|+(\w+)(?:/\w+)?\}\}(?:[+<br />]+\{\{(?:key|tag)\|([:_\w]+)\|+(\w+)\}\})?.*
?.*
?\|(\d+)\}\}''',
],[
    lambda x: [x[0],x[1],x[2],x[3],"yes",None,None,x[4],"dkey_dvalue_yes"],
    r'''\{\{Deprecated features/item\|lang=\{\{\{lang\|\}\}\}\|date=([\d-]*).*
\|dkey=([:_\w]+)\|dvalue ?= ?([_\w]+)\|?.*
\|suggestion=\{\{(?:key|tag)\|([:_\w]+)\|*\}\}.*
?.*
?\|(\d+)\}\}''',
],[
    lambda x: [x[0],x[1],x[2],x[3],"yes",None,None,x[4],"dkey_dvalue_square_yes"],
    r'''\{\{Deprecated features/item\|lang=\{\{\{lang\|\}\}\}\|date=([\d-]*).*
\|dkey=([:_\w]+)\|dvalue ?= ?([_\w]+)\|?.*
\|suggestion=\[\[(?:key|tag):(\w+).+\]\].*
?.*
?\|(\d+)\}\}''',
],[
    lambda x: [x[0],x[1],CARRY_VALUE,x[2],x[3],x[4],CARRY_VALUE,x[5],"dkey_fixed_carry"],
    r'''\{\{Deprecated features/item\|lang=\{\{\{lang\|\}\}\}\|date=([\d-]*).*
\|dkey=([:_\w]+)
\|suggestion=\{\{(?:key|tag)\|([:_\w]+)\|+(\w+)(?:/\w+)?\}\}(?:[+<br />]+\{\{(?:key|tag)\|([:_\w]+)\}\})?.*
?.*
?\|(\d+)\}\}''',
],[
    lambda x: [x[0],x[1],CARRY_VALUE,x[2],CARRY_VALUE,None,None,x[3],"dkey_carry"],
    r'''\{\{Deprecated features/item\|lang=\{\{\{lang\|\}\}\}\|date=([\d-]*).*
\|dkey=([:_\w]+)
\|suggestion=\{\{(?:key|tag)\|+([:_\w]+)\|?\}\}.*
?.*
?\|(\d+)\}\}''',
]]

def convert_deprecated_wiki_to_df(wiki_txt:str)->pd.DataFrame:
    return pd.DataFrame(
        [
            row 
            for expr in DEPRECATED_FEATURES_REGEX
            for row in map(expr[0], re.findall(expr[1], wiki_txt, flags=re.IGNORECASE))
        ],
        columns=[
            "date","old_key","old_value","new_key_1","new_value_1","new_key_2","new_value_2","id","regex_type"
        ]
    )

def create_deprecated_df():
    wiki_file_name = f"deprecated.wiki"

    if not path.exists(wiki_file_name):
        download_deprecated_wiki(wiki_file_name)

    with open(wiki_file_name, "r") as wiki_file:
        wiki = wiki_file.read()

    return convert_deprecated_wiki_to_df(wiki)

class DeprecatedCounterHandler(osmium.SimpleHandler):
    def __init__(self, deprecated_df:pd.DataFrame, writer:osmium.SimpleWriter):
        super(DeprecatedCounterHandler, self).__init__()
        self.deprecated_df = deprecated_df
        self.carry_value = self.deprecated_df["old_value"] == CARRY_VALUE
        self.deprecated_key_set = set(deprecated_df["old_key"]) # Used for faster lookup
        self.actions = []
        self.writer = writer

    def transform(self, type:str, id:int, tags:osmium.osm.TagList):
        ret = True
        for k,v in tags:
            if k in self.deprecated_key_set:
                deprecated_mask = (self.deprecated_df["old_key"]==k) & (self.carry_value | (self.deprecated_df["old_value"]==v))
                if deprecated_mask.any():
                    print(tags)
                    #deprecated_row = self.deprecated_df[deprecated_mask][0]
                    self.actions.append(["deprecated", type, id, k, v, "update_tag"])
                    return False #TODO actually update
            
            if k=="wikidata" and not re.match(r"^Q\d+$", v):
                print(tags)
                self.actions.append(["bad_wikidata", type, id, k, v, "delete_tag"])
                return False #TODO actually update
        
        return ret

    def node(self, n):
        tags = self.transform("node", n.id, n.tags)
        if tags is False:
            return
        elif tags is True:
            self.writer.add_node(n)
        else:
            self.writer.add_node(n.replace(tags=tags))

    def way(self, w):
        tags = self.transform("way", w.id, w.tags)
        if tags is False:
            return
        elif tags is True:
            self.writer.add_way(w)
        else:
            self.writer.add_way(w.replace(tags=tags))

    def relation(self, r):
        tags = self.transform("relation", r.id, r.tags)
        if tags is False:
            return
        elif tags is True:
            self.writer.add_relation(r)
        else:
            self.writer.add_relation(r.replace(tags=tags))

print("Start: ", datetime.now().isoformat())
deprecated_csv_file_name = "deprecated.csv"
if path.exists(deprecated_csv_file_name):
    deprecated_df = pd.read_csv(deprecated_csv_file_name)
else:
    deprecated_df = create_deprecated_df()
    deprecated_df.to_csv(deprecated_csv_file_name)

#pbf_url = "http://download.geofabrik.de/europe/italy/nord-est-221226.osm.pbf" # About 500MB
pbf_url = "http://download.geofabrik.de/europe/moldova-221226.osm.pbf" # About 62MB
#pbf_url = "http://download.geofabrik.de/europe/luxembourg-221226.osm.pbf" # About 37MB
pbf_path = path.basename(pbf_url)

if not path.exists(pbf_path):
    request.urlretrieve(pbf_url, pbf_path)

out_pbf_path = f"{pbf_path}.opinionated.osm.pbf"
if not path.exists(out_pbf_path):
    writer = osmium.SimpleWriter(out_pbf_path)

    handler = DeprecatedCounterHandler(deprecated_df, writer)

    print("Pre-elaboration:", datetime.now().isoformat())
    handler.apply_file(pbf_path)
    print("Post-elaboration:", datetime.now().isoformat())

    actions = pd.DataFrame(handler.actions, columns=["error","type","id","key","value","action"])
    print(actions.describe())

    actions_csv_path = f"{pbf_path}.actions.csv"
    actions.to_csv(actions_csv_path)