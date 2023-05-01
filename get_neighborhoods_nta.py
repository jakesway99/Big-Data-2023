import json
import csv
import os

# gets the neighborhoods from the NTA geojson

with open("NTA_json.geojson") as f:
    data = json.load(f)

nta_names = []
for feature in data["features"]:
    nta_names.append((feature["properties"]["ntacode"], feature["properties"]["ntaname"]))

all_ntanames = set(nta_names)
path = os.path.dirname(os.path.realpath(__file__))
output_file = os.path.join(path, "all_ntanames.csv")

if not os.path.exists(output_file):
    with open(output_file, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["ntacode", "ntaname"])
        for ntacode, ntaname in all_ntanames:
            writer.writerow([ntacode, ntaname])

