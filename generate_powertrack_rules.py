#!/usr/bin/python

"""
From a list of Twitter user objects, create GNIP PowerTrack rules
that search for tweets from the handles and mentions of them.
Outputs /rule payloads in JSON after chunking the rules into groups of
no more than MAX_UPDATE_SIZE_CHARS characters each.
"""

import json

import argparse

# For each handle, a pair of rules will be added. PowerTrack /rules payload max is 2048 chars.
# The limit below keeps us comfortably below that.
NUM_HANDLES_PER_RULE = 30


def make_rule(rule, tag):
    """Given a search and a tag, format it as a PowerTrack rule"""
    return {"value": rule, "tag": tag}


def generate(user_record_list, ruleset_name):
    """Given a list of user records, return a list of PowerTrack /rule updates"""
    return handles_to_rules(
        [user["screen_name"] for user in user_record_list], ruleset_name
    )


def handles_to_rules(handles, ruleset_name):
    """Given a list of Twitter user IDs, return a list of PowerTrack /rule updates"""
    output_updates = []

    # Output NUM_HANDLES_PER_RULE at a time
    for i in range(0, len(handles), NUM_HANDLES_PER_RULE):
        handle_subset = handles[i : min(len(handles), i + NUM_HANDLES_PER_RULE)]
        output_updates.append(
            {
                "rules": [
                    make_rule(
                        " OR ".join(["from:%s" % (s) for s in handle_subset]),
                        "from_%s_%d" % (ruleset_name, i / NUM_HANDLES_PER_RULE),
                    ),
                    make_rule(
                        " OR ".join(["@%s" % (s) for s in handle_subset]),
                        "at_%s_%d" % (ruleset_name, i / NUM_HANDLES_PER_RULE),
                    ),
                    make_rule(
                        " OR ".join(["retweets_of:%s" % (s) for s in handle_subset]),
                        "retweets_of_%s_%d" % (ruleset_name, i / NUM_HANDLES_PER_RULE),
                    ),
                ]
            }
        )
    return output_updates
