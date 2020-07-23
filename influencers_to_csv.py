#!/usr/bin/python3

"""This script makes a CSV file (suitable for importing into a spreadsheet)
from the results of ./expand_influencers.py.

Example:

  cat seed_sets/boston_seed_set.expanded_set.json | ./influencers_to_csv.py > out.csv
"""

import json
import sys
import csv

rows = []
for line in sys.stdin:
    x = json.loads(line)
    # Only keep the most popular users
    if x["followers_count"] < 20:
        continue
    # Only keep the most relevant users
    if x["civic_odds_ratio"] < 200:
        continue

    # Metric to sort by.  (Could be "civic_odds_ratio" instead, or something else.)
    metric = x["followers_count"]
    rows.append((metric, x))

rows.sort(key = lambda x: x[0], reverse=True)
csvout = csv.writer(sys.stdout)
csvout.writerow(["Screen name", "Display name", "Location",
                 "Followers", "Relevance", "Description"])
for r in rows[:500]:
    csvout.writerow([r[1]["screen_name"], r[1]["name"],
                     r[1]["location"], r[1]["followers_count"],
                     r[1]["civic_odds_ratio"],
                     r[1]["description"]])
