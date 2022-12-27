from urllib import request
import re
import json
from datetime import datetime
from os import path

def download_deprecated_wiki(wiki_file_name:str):
    url = "https://wiki.openstreetmap.org/w/api.php?action=query&format=json&titles=Template%3ADeprecated_features&redirects=0&prop=revisions&rvprop=content&indexpageids=1"
    with request.urlopen(url) as json_response:
        response = json.load(json_response)
    pageID = response["query"]["pageids"][0]
    csv = response["query"]["pages"][pageID]["revisions"][0]["*"]
    with open(wiki_file_name, "w") as wiki_file:
        wiki_file.write(csv)

def convert_deprecated_wiki_to_csv(wiki_txt:str)->str:
    # Remove head
    csv = wiki_txt.replace(
"""{|class="wikitable sortable"
|-
!scope="col" style="width:7em"| {{{date|Date}}}
!scope="col"| {{{old_kv|Deprecated key/value}}}
!scope="col" data-sort-type="number" | {{{usage|Current usage}}}
!scope="col"| {{{suggestion|Suggestion of replacement}}}
!scope="col" style="width:1.5em"| N
!scope="col"| {{{reason|Reason}}}

""", ""
        ).replace("|}<noinclude>{{Documentation}}</noinclude>", "")

    csv = re.sub(
r'''\{\{Deprecated features/item\|lang=\{\{\{lang\|\}\}\}\|date=([\d-]*).*
\|dkey=([:_\w]+)\|dvalue ?= ?([\*_\w]+)\|?.*
\|suggestion=\{\{(?:key|tag)\|([:_\w]+)\|+(\w+)(?:/\w+)?\}\}[+<br />]+\{\{(?:key|tag)\|([:_\w]+)\|*\}\}.*
?.*
?\|(\d+)\}\}''', "\\1,\\2,\\3,\\4,\\5,\\6,yes,\\7,fixed_value_yes", csv, count=0, flags=re.IGNORECASE)

    csv = re.sub(
r'''\{\{Deprecated features/item\|lang=\{\{\{lang\|\}\}\}\|date=([\d-]*).*
\|dkey=([:_\w]+)\|dvalue ?= ?([\*_\w]+)\|?.*
\|suggestion=\{\{(?:key|tag)\|([:_\w]+)\|+(\w+)(?:/\w+)?\}\}(?:[+<br />]+\{\{(?:key|tag)\|([:_\w]+)\|*(\w+)\}\})?.*
?.*
?\|(\d+)\}\}''', "\\1,\\2,\\3,\\4,\\5,\\6,\\7,\\8,fixed_value", csv, count=0, flags=re.IGNORECASE)

    csv = re.sub(
r'''\{\{Deprecated features/item\|lang=\{\{\{lang\|\}\}\}\|date=([\d-]*).*
\|dkey=([:_\w]+)\|dvalue ?= ?([\*_\w]+)\|?.*
\|suggestion=\{\{(?:key|tag)\|([:_\w]+)\|*\}\}.*
?.*
?\|(\d+)\}\}''', "\\1,\\2,\\3,\\4,yes,,,\\5,value_yes", csv, count=0, flags=re.IGNORECASE)

    csv = re.sub(
r'''\{\{Deprecated features/item\|lang=\{\{\{lang\|\}\}\}\|date=([\d-]*).*
\|dkey=([:_\w]+)\|dvalue ?= ?([_\w]+)\|?.*
\|suggestion=\[\[(?:key|tag):(\w+).+\]\].*
?.*
?\|(\d+)\}\}''', "\\1,\\2,\\3,\\4,yes,,,\\5,square_value_yes", csv, count=0, flags=re.IGNORECASE)

    csv = re.sub(
r'''\{\{Deprecated features/item\|lang=\{\{\{lang\|\}\}\}\|date=([\d-]*).*
\|dkey=([:_\w]+)
\|suggestion=\{\{(?:key|tag)\|([:_\w]+)\|+(\w+)(?:/\w+)?\}\}(?:[+<br />]+\{\{(?:key|tag)\|([:_\w]+)\}\})?.*
?.*
?\|(\d+)\}\}''', "\\1,\\2,_VALUE_,\\3,\\4,\\5,_VALUE_,\\6,carry_fixed_value", csv, count=0, flags=re.IGNORECASE)

    csv = re.sub(
r'''\{\{Deprecated features/item\|lang=\{\{\{lang\|\}\}\}\|date=([\d-]*).*
\|dkey=([:_\w]+)
\|suggestion=\{\{(?:key|tag)\|+([:_\w]+)\|?\}\}.*
?.*
?\|(\d+)\}\}''', "\\1,\\2,_VALUE_,\\3,_VALUE_,,,\\4,carry_value", csv, count=0, flags=re.IGNORECASE)

    csv = re.sub(
r'''\{\{Deprecated features/item\|lang=\{\{\{lang\|\}\}\}\|date=([\d-]*).*
\|dkey=([:_\w]+)(?:\|dvalue ?= ?([\*_\w]+))?\|?.*
\|suggestion=(?:Specific values|Look |Relation|Use relation|various|No( value)?|(See )?\[\[[ #\w]+\]\]).*\n?.*
\|(\d+)\}\}
''', "", csv, count=0, flags=re.IGNORECASE)

    csv = f"date,old_key,old_value,new_key_1,new_value_1,new_key_2,new_value_2,id,regex_type\n{csv}"
    return csv

def create_deprecated_csv(csv_file_name):
    wiki_file_name = f"deprecated.wiki"

    if not path.exists(wiki_file_name):
        download_deprecated_wiki(wiki_file_name)

    with open(wiki_file_name, "r") as wiki_file:
        wiki = wiki_file.read()

    csv = convert_deprecated_wiki_to_csv(wiki)

    with open(csv_file_name, "w") as csv_file:
        csv_file.write(csv)

csv_file_name = f"deprecated.{datetime.now().isoformat()}.csv"
create_deprecated_csv(csv_file_name)

#load deprecated

#apply deprecation changes
