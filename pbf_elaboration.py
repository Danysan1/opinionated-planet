from datetime import datetime
import pandas as pd
from osmium import SimpleHandler, SimpleWriter, osm
from deprecated_features import (
        DEPRECATED_CARRY_VALUE,
        DEPRECATED_OLD_KEY,
        DEPRECATED_OLD_VALUE,
        DEPRECATED_NEW_KEY_1,
        DEPRECATED_NEW_VALUE_1,
        DEPRECATED_NEW_KEY_2,
        DEPRECATED_NEW_VALUE_2,
        DEPRECATED_REGEX_TYPE,
        DEPRECATED_DKEY_DVALUE_FIXED,
        DEPRECATED_DKEY_DVALUE_FIXED_FIXED,
        DEPRECATED_DKEY_DVALUE_YES,
        DEPRECATED_DKEY_FIXED_CARRY,
        DEPRECATED_DKEY_CARRY,
        get_deprecated_df
    )
from wikidata import get_wikidata_labels_df
from os import path, remove

class PbfElaborationHandler(SimpleHandler):
    def __init__(self, writer:SimpleWriter, deprecated_df:pd.DataFrame, labels_df:pd.DataFrame):
        super(PbfElaborationHandler, self).__init__()

        self.deprecated_df = deprecated_df
        self.carry_value = self.deprecated_df[DEPRECATED_OLD_VALUE] == DEPRECATED_CARRY_VALUE
        self.full_deprecated_key_set = set(deprecated_df[DEPRECATED_OLD_KEY]) # Used for faster lookup
        
        self.labels_df = labels_df
        self.labels = labels_df["label"].str.lower()

        self.actions = []
        self.writer = writer
        self.count = 0

    def update_deprecated_tags(self, type:str, id:int, keys:set, tags:osm.TagList):
        if not tags:
            return tags
        
        ret = tags
        deprecated_key_set = keys & self.full_deprecated_key_set
        if deprecated_key_set:
            for key in deprecated_key_set:
                value = tags.get(key)
                deprecated_mask = (self.deprecated_df[DEPRECATED_OLD_KEY]==key) & (self.carry_value | (self.deprecated_df[DEPRECATED_OLD_VALUE]==value))
                if deprecated_mask.any():
                    deprecated_row = self.deprecated_df[deprecated_mask].iloc[0]
                    key1 = deprecated_row[DEPRECATED_NEW_KEY_1]
                    regex_type = deprecated_row[DEPRECATED_REGEX_TYPE]
                    ret = dict(tags)
                    if regex_type == DEPRECATED_DKEY_DVALUE_FIXED:
                        value1 = deprecated_row[DEPRECATED_NEW_VALUE_1]
                        del ret[key]
                        ret[key1] = value1
                        self.actions.append([type, id, key, value, "update_deprecated_tag", f"{key1}={value1}"])
                    elif regex_type == DEPRECATED_DKEY_DVALUE_FIXED_FIXED:
                        value1 = deprecated_row[DEPRECATED_NEW_VALUE_1]
                        key2 = deprecated_row[DEPRECATED_NEW_KEY_2]
                        value2 = deprecated_row[DEPRECATED_NEW_VALUE_2]
                        del ret[key]
                        ret[key1] = value1
                        ret[key2] = value2
                        self.actions.append([type, id, key, value, "update_deprecated_tag", f"{key1}={value1} + {key2}={value2}"])
                    elif regex_type == DEPRECATED_DKEY_DVALUE_YES:
                        del ret[key]
                        ret[key1] = "yes"
                        self.actions.append([type, id, key, value, "update_deprecated_tag", f"{key1}=yes"])
                    elif regex_type == DEPRECATED_DKEY_FIXED_CARRY:
                        value1 = deprecated_row[DEPRECATED_NEW_VALUE_1]
                        key2 = deprecated_row[DEPRECATED_NEW_KEY_2]
                        carry_value = ret.pop(key)
                        ret[key1] = value1
                        ret[key2] = carry_value
                        self.actions.append([type, id, key, value, "update_deprecated_tag", f"{key1}={value1} + {key2}={carry_value}"])
                    elif regex_type == DEPRECATED_DKEY_CARRY:
                        carry_value = ret.pop(key)
                        ret[key1] = carry_value
                        self.actions.append([type, id, key, value, "update_deprecated_tag", f"{key1}={carry_value}"])
        return ret

    def add_wikidata_label(self, type:str, id:int, keys:set, tags:osm.TagList):
        if not tags:
            return tags
        
        ret = tags
        if tags and "wikidata" in keys:
            value = tags.get("wikidata")
            labels_mask = (self.labels_df["id"] == value) & ~(self.labels_df["key"].isin(keys))
            if "name" in tags:
                name = tags.get("name")
                labels_mask &= (self.labels != name.lower())

            if labels_mask.any():
                labels:pd.DataFrame = self.labels_df[labels_mask]
                ret = dict(tags)
                labels.apply(lambda row: ret.update({row["key"]: row["label"]}), axis=1)
                self.actions.append([type, id, "wikidata", value, "add_wikidata_label", ",".join(labels["lang"])])
        return ret

    def transform(self, type:str, id:int, tags:osm.TagList):
        self.count+=1
        if self.count % 1_000_000 == 0:
            print(f"Analysed elements: {self.count}")

        key_set = set(dict(tags))

        ret = tags
        ret = self.update_deprecated_tags(type, id, key_set, ret)
        ret = self.add_wikidata_label(type, id, key_set, ret)
        
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
    try:
        writer = SimpleWriter(out_pbf_path)
        handler = PbfElaborationHandler(writer, deprecated_df, labels_df)

        print("Elaboration started at", datetime.now().isoformat())
        handler.apply_file(in_pbf_path)
    except Exception as err:
        print("Elaboration failed at", datetime.now().isoformat())
        remove(out_pbf_path)
        raise err
    print("Elaboration finished at", datetime.now().isoformat())

    actions_df = pd.DataFrame(handler.actions, columns=["type","id","key","value","action","details"])
    actions_df["url"] = "https://www.openstreetmap.org/" + actions_df["type"] + "/" + actions_df["id"].astype("string")
    return actions_df

def elaborate_pbf(in_pbf_path:str, skip_cache:bool=False):
    out_pbf_path = f"{in_pbf_path}.opinionated.osm.pbf"
    if path.exists(out_pbf_path) and not skip_cache:
        print("Output PBF already exists, skipping elaboration")
    else:
        if path.exists(out_pbf_path):
            remove(out_pbf_path)
        actions_df = elaborate_pbf_and_get_actions_df(in_pbf_path, out_pbf_path, skip_cache)
        print("Actions DataFrame:\n", actions_df.describe(include = 'all'))

        actions_csv_path = f"{in_pbf_path}.actions.csv"
        actions_df.to_csv(actions_csv_path)
