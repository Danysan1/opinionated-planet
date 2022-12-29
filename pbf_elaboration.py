from urllib import request
import re
import json
from datetime import datetime
import pandas as pd
import osmium
from deprecated_features import CARRY_VALUE, get_deprecated_df
from wikidata import get_wikidata_labels_df
from os import path, remove

class PbfElaborationHandler(osmium.SimpleHandler):
    def __init__(self, writer:osmium.SimpleWriter, deprecated_df:pd.DataFrame, labels_df:pd.DataFrame):
        super(PbfElaborationHandler, self).__init__()

        self.deprecated_df = deprecated_df
        self.carry_value = self.deprecated_df["old_value"] == CARRY_VALUE
        self.deprecated_key_set = set(deprecated_df["old_key"]) # Used for faster lookup
        
        self.labels_df = labels_df
        self.actions = []
        self.writer = writer
        self.count = 0

    def update_deprecated_tags(self, type:str, id:int, tags):
        ret = tags
        for key,value in tags:
            if key in self.deprecated_key_set:
                deprecated_mask = (self.deprecated_df["old_key"]==key) & (self.carry_value | (self.deprecated_df["old_value"]==value))
                if deprecated_mask.any():
                    deprecated_row = self.deprecated_df[deprecated_mask].iloc[0]
                    key1 = deprecated_row["new_key_1"]
                    value1 = deprecated_row["new_value_1"]
                    self.actions.append([
                        type, id, key, value, "update_deprecated_tag", f"{key1}={value1}"
                    ])
                    return False #TODO actually update
        return ret

    def add_wikidata_label(self, type:str, id:int, tags):
        ret = tags
        value = tags.get("wikidata")
        name = tags.get("name")
        labels:pd.DataFrame = self.labels_df[self.labels_df["id"] == value]
        labels = labels[labels["label"] != name]
        labels = labels[~(labels["key"].isin(set(tags)))]
        if not labels.empty:
            ret = dict(tags)
            labels.apply(lambda row: ret.update({row["key"]: row["label"]}), axis=1)
            self.actions.append([type, id, "wikidata", value, "add_wikidata_label", ", ".join(labels["lang"])])
        return ret

    def transform(self, type:str, id:int, tags:osmium.osm.TagList):
        self.count+=1
        if self.count % 1_000_000 == 0:
            print(f"Analysed elements: {self.count}")

        ret = tags
        if set(tags) & self.deprecated_key_set:
            self.update_deprecated_tags(type, id, ret)
        
        if "wikidata" in tags:
            ret = self.add_wikidata_label(type, id, ret)
        
        return True if ret is tags else ret

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

def elaborate_pbf_and_get_actions_df(in_pbf_path:str, out_pbf_path:str, skip_cache:bool=False):
    print(f"Elaborating {in_pbf_path} to {out_pbf_path}")
    deprecated_df = get_deprecated_df()
    labels_df = get_wikidata_labels_df(in_pbf_path, skip_cache)
    writer = osmium.SimpleWriter(out_pbf_path)
    handler = PbfElaborationHandler(writer, deprecated_df, labels_df)

    print("Pre-elaboration:", datetime.now().isoformat())
    handler.apply_file(in_pbf_path)
    print("Post-elaboration:", datetime.now().isoformat())

    actions_df = pd.DataFrame(handler.actions, columns=["type","id","key","value","action","details"])
    actions_df["url"] = "https://www.openstreetmap.org/" + actions_df["type"] + "/" + actions_df["id"].astype("string")
    return actions_df

def elaborate_pbf(in_pbf_path:str, skip_cache:bool=False):
    out_pbf_path = f"{in_pbf_path}.opinionated.osm.pbf"
    if path.exists(out_pbf_path) and not skip_cache:
        print("Output PBF already exists, skipping elaboration")
    else:
        remove(out_pbf_path)
        actions_df = elaborate_pbf_and_get_actions_df(in_pbf_path, out_pbf_path, skip_cache)
        print("Actions DataFrame:\n", actions_df.describe(include = 'all'))

        actions_csv_path = f"{in_pbf_path}.actions.csv"
        actions_df.to_csv(actions_csv_path)
