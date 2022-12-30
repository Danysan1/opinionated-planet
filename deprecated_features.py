from urllib import request
import re
import json
from os import path
import pandas as pd
import logging

def download_deprecated_wiki(wiki_file_name:str) -> str:
    url = "https://wiki.openstreetmap.org/w/api.php?action=query&format=json&titles=Template%3ADeprecated_features&redirects=0&prop=revisions&rvprop=content&indexpageids=1"
    with request.urlopen(url) as json_response:
        response = json.load(json_response)
    pageID = response["query"]["pageids"][0]
    wiki_text:str = response["query"]["pages"][pageID]["revisions"][0]["*"]
    with open(wiki_file_name, "w") as wiki_file:
        wiki_file.write(wiki_text)
    
    return wiki_text

DEPRECATED_REGEX_TYPE = "regex_type"
DEPRECATED_DKEY_DVALUE_FIXED = "dkey_dvalue_fixed"
DEPRECATED_DKEY_DVALUE_FIXED_FIXED = "dkey_dvalue_fixed_fixed"
DEPRECATED_DKEY_DVALUE_YES = "dkey_dvalue_yes"
DEPRECATED_DKEY_FIXED_CARRY = "dkey_fixed_carry"
DEPRECATED_DKEY_CARRY = "dkey_carry"
DEPRECATED_CARRY_VALUE = "__CARRY__"
DEPRECATED_OLD_KEY = "old_key"
DEPRECATED_OLD_VALUE = "old_value"
DEPRECATED_NEW_KEY_1 = "new_key_1"
DEPRECATED_NEW_VALUE_1 = "new_value_1"
DEPRECATED_NEW_KEY_2 = "new_key_2"
DEPRECATED_NEW_VALUE_2 = "new_value_2"

DEPRECATED_FEATURES_REGEX = [[
    lambda x: [x[0],x[1],x[2],x[3],x[4],None,None,x[5],DEPRECATED_DKEY_DVALUE_FIXED],
    r'''\{\{Deprecated features\/item\|lang=\{\{\{lang\|\}\}\}\|date=([\d-]*).*
\|dkey=([:_\w]+)\|dvalue ?= ?([_\w]+)(?:\|dvalue_nolink=.+)?
\|suggestion=\{\{(?:key|tag)\|([:_\w]+)\|+([_\w]+)(?:\/\w+)?\}\}[^+\n]*(?:
.*)?
?\|(\d+)\}\}''',
],[
    lambda x: [x[0],x[1],x[2],x[3],x[4],x[5],x[6],x[7],DEPRECATED_DKEY_DVALUE_FIXED_FIXED],
    r'''\{\{Deprecated features\/item\|lang=\{\{\{lang\|\}\}\}\|date=([\d-]*).*
\|dkey=([:_\w]+)\|dvalue ?= ?([_\w]+)(?:\|dvalue_nolink=.+)?
\|suggestion=\{\{(?:key|tag)\|([:_\w]+)\|+([_\w]+)(?:\/\w+)?\}\}[+<br />]+\{\{(?:key|tag)\|([:_\w]+)\|+([_\w]+)\}\}.*
?.*
?\|(\d+)\}\}''',
],[
    lambda x: [x[0],x[1],x[2],x[3],"yes",None,None,x[4],DEPRECATED_DKEY_DVALUE_YES],
    r'''\{\{Deprecated features\/item\|lang=\{\{\{lang\|\}\}\}\|date=([\d-]*).*
\|dkey=([:_\w]+)\|dvalue ?= ?([_\w]+)(?:\|dvalue_nolink=.+)?
\|suggestion=\{\{(?:key|tag)\|([_\w]+)\|*\}\}.*
?.*
?\|(\d+)\}\}''',
],[
    lambda x: [x[0],x[1],x[2],x[3],"yes",None,None,x[4],DEPRECATED_DKEY_DVALUE_YES],
    r'''\{\{Deprecated features\/item\|lang=\{\{\{lang\|\}\}\}\|date=([\d-]*).*
\|dkey=([:_\w]+)\|dvalue ?= ?([_\w]+)(?:\|dvalue_nolink=.+)?
\|suggestion=\[\[(?:key|tag):([_\w]+).+\]\].*
?.*
?\|(\d+)\}\}''',
],[
    lambda x: [x[0],x[1],DEPRECATED_CARRY_VALUE,x[2],x[3],x[4],DEPRECATED_CARRY_VALUE,x[5],DEPRECATED_DKEY_FIXED_CARRY],
    r'''\{\{Deprecated features\/item\|lang=\{\{\{lang\|\}\}\}\|date=([\d-]*).*
\|dkey=([:_\w]+)(?:\|dvalue_nolink=.+)?
\|suggestion=\{\{(?:key|tag)\|([:_\w]+)\|+([_\w]+)(?:\/\w+)?\}\}(?:[+<br />]+\{\{(?:key|tag)\|([:_\w]+)\}\})?.*
?.*
?\|(\d+)\}\}''',
],[
    lambda x: [x[0],x[1],DEPRECATED_CARRY_VALUE,x[2],DEPRECATED_CARRY_VALUE,None,None,x[3],DEPRECATED_DKEY_CARRY],
    r'''\{\{Deprecated features\/item\|lang=\{\{\{lang\|\}\}\}\|date=([\d-]*).*
\|dkey=([:_\w]+)(?:\|dvalue_nolink=.+)?
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
            "date",
            DEPRECATED_OLD_KEY,
            DEPRECATED_OLD_VALUE,
            DEPRECATED_NEW_KEY_1,
            "new_value_1",
            "new_key_2",
            "new_value_2",
            "id",
            DEPRECATED_REGEX_TYPE
        ]
    )

def create_deprecated_df(skip_cache:bool=False):
    wiki_file_name = f"deprecated.wiki"

    if skip_cache or not path.exists(wiki_file_name):
        wiki_text = download_deprecated_wiki(wiki_file_name)
    else:
        with open(wiki_file_name, "r") as wiki_file:
            wiki_text = wiki_file.read()

    return convert_deprecated_wiki_to_df(wiki_text)

def get_deprecated_df():
    csv_path = "deprecated.csv"
    if not path.exists(csv_path):
        df = create_deprecated_df()
        df.to_csv(csv_path)
    else:
        df = pd.read_csv(csv_path)
    df[DEPRECATED_REGEX_TYPE] = df[DEPRECATED_REGEX_TYPE].astype("category")
    
    logging.info("Deprecated features DataFrame:\n%s", df.describe(include = 'all'))
    return df