#!/usr/bin/env python3

"""Given a "seed set" of Twitter handles that tightly pertain to some
city and/or topic, this script runs a two-step follower graph analysis
that yields an expanded set of Twitter handles.  The first step is
to compute a fixed-size set of "civic listeners", the users who follow
the greatest number of members of the seed set.  The second step is to
compute the set of users frequently followed by these listeners, which
form the basis of the "civic influencers" set after some filtering.
Some constants controlling aspects of the pull are below.

Example:

  ./src/python/twitter/expand_influencers.py --seedfile seed_sets/boston_seed_set.txt

The environment variables TWITTER_API_KEY and TWITTER_SECRET must be
set to your respective Twitter API credentials.

The script will output a file of Twitter user objects, as well as a
file of rules for Twitter PowerTrack that follows these users.

(Note that this script will make several thousand Twitter API queries and
occasionally have to wait on the rate limit, especially during the
final user-metadata-lookup part.  It takes about an hour for the
Boston seed set at the default parameters.)
"""

import argparse
import json
import logging
import os

import tweepy

import generate_powertrack_rules

API_KEY = os.environ["TWITTER_API_KEY"]
SECRET = os.environ["TWITTER_SECRET"]

# Output path
OUTPUT_PATH = "."

# Number of "civic listeners", an intermediate output
LISTENER_SET_SIZE = 100

# Max number of "civic influencers", the final output
INFLUENCER_SET_SIZE = 1000

# For efficiency, only consider the first MAX_FOLLOWERS followers of a user
# when doing the first step.
MAX_FOLLOWERS = 100000

# For efficiency, only consider the first MAX_FRIENDS followees of a user
# when doing the second step.
MAX_FRIENDS = 100000

# Estimate of the number of Twitter users total.  This is used to estimate
# the odds ratio for the "specificity score" computer for each influencer.
NUM_TWITTER_USERS = 5e8

# If the odds ratio (odds of being followed by a civic listener, divided by odds
# of being followed by an average Twitter user) is below this threshold, do not
# output this influencer.  This keeps out handles that are extremely popular
# but not directly relevant to the city or topic.
MIN_CIVIC_ODDS_RATIO = 10


def read_seed_set(fname):
    """Reads a file of Twitter handles, one per line;  only the first token on each line is used.
    """
    lines = open(fname).read().split("\n")
    return list(
        set([line.strip().replace("@", "").split()[0] for line in lines if line])
    )


def handle_to_followers(tweepy_api, handle):
    """Given a Twitter handle, return the user IDs of its followers.
    """
    try:
        items = tweepy.Cursor(tweepy_api.followers_ids, screen_name=handle).items(
            MAX_FOLLOWERS
        )
        return list(items)
    except tweepy.error.TweepError:
        LOGGER.warning("Can't get followers for: %s", handle)
        return []


def id_to_followees(tweepy_api, twitter_id):
    """Given a Twitter user ID, return the IDs of its followees/"friends".
    """
    try:
        items = tweepy.Cursor(tweepy_api.friends_ids, id=twitter_id).items(MAX_FRIENDS)
        return list(items)
    except tweepy.error.TweepError:
        LOGGER.warning("Can't get followees for: %s", twitter_id)
        return []


def id_to_user_metadata(tweepy_api, twitter_id):
    """Given a Twitter user ID, return user record.
    """
    try:
        return tweepy_api.get_user(twitter_id)
    except tweepy.error.TweepError:
        LOGGER.warning("Can't get user data for: %s", twitter_id)
        return None


def add_civic_stats(user_record, num_civic_listeners):
    """Add civic_listeners and civic_odds_ratio fields to the user record JSON
    """
    user_record._json["civic_listeners"] = num_civic_listeners
    num_followers = user_record.followers_count
    if not num_followers:
        return
    civic_odds = num_civic_listeners / float(
        INFLUENCER_SET_SIZE + 1 - num_civic_listeners
    )
    general_odds = num_followers / (NUM_TWITTER_USERS - num_followers)
    odds_ratio = civic_odds / general_odds
    user_record._json["civic_odds_ratio"] = odds_ratio


def get_expanded_users(tweepy_api, seed_filename):
    """The main show:  Finds the expanded set of influencers given a seed set file
    """
    seed_set = read_seed_set(seed_filename)
    LOGGER.info("Seed set contains %d entries", len(seed_set))

    LOGGER.info("Determining civic listener set")
    follower_to_seed_handle_set = {}  # follower ID -> set of seed set handles followed
    for handle in seed_set:
        for follower_id in handle_to_followers(tweepy_api, handle):
            if follower_id not in follower_to_seed_handle_set:
                follower_to_seed_handle_set[follower_id] = set()
            follower_to_seed_handle_set[follower_id].add(handle)

    f_items = list(follower_to_seed_handle_set.items())
    # Sort by size of handle set, descending
    f_items.sort(key=lambda x: len(x[1]), reverse=True)
    LOGGER.info(
        "%d items in raw listener set; most engaged follows %d seed members",
        len(f_items),
        len(f_items[0][1]),
    )
    civic_listeners = [(item[0], len(item[1])) for item in f_items[:LISTENER_SET_SIZE]]

    LOGGER.info("Determining followees of civic listener set")
    influencer_to_listener_set = {}  # influencer ID -> set of listener IDs
    for listener_id, _ in civic_listeners:
        for followee_id in id_to_followees(tweepy_api, listener_id):
            if followee_id not in influencer_to_listener_set:
                influencer_to_listener_set[followee_id] = set()
            influencer_to_listener_set[followee_id].add(listener_id)
    f_items = list(influencer_to_listener_set.items())
    # Sort by size of ID set, descending
    f_items.sort(key=lambda x: len(x[1]), reverse=True)
    LOGGER.info(
        "%d items in raw influencer set; most influential handle followed by %d listeners",
        len(f_items),
        len(f_items[0][1]),
    )
    civic_influencers = [
        (item[0], len(item[1])) for item in f_items[:INFLUENCER_SET_SIZE]
    ]

    LOGGER.info("Getting user metadata")
    output_records = []
    for influencer_id, num_civic_listeners in civic_influencers:
        user_record = id_to_user_metadata(tweepy_api, influencer_id)
        if user_record:
            add_civic_stats(user_record, num_civic_listeners)
            if user_record._json["civic_odds_ratio"] >= MIN_CIVIC_ODDS_RATIO:
                # Output this record
                output_records.append(user_record._json)
    return output_records


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Generate a list of influencers from a seed set"
    )
    parser.add_argument(
        "--seedfile",
        type=str,
        required=True,
        help="Path of file containing seed set, one Twitter handle per line",
    )
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
    LOGGER = logging.getLogger("expand_influencers")

    expanded_users = get_expanded_users(
        tweepy.API(tweepy.AppAuthHandler(API_KEY, SECRET), wait_on_rate_limit=True),
        args.seedfile,
    )

    # Write users to file
    base_name = args.seedfile.replace(".txt", "")
    output_fname = os.path.join(OUTPUT_PATH, base_name + ".expanded_set.json")
    with open(output_fname, "w") as fs:
        print("\n".join([json.dumps(user_record) for user_record in expanded_users]),
              file=fs)
    logging.info("Users rules in %s", output_fname)

    # Write Powertrack rules to file
    ruleset_name = base_name.split("/")[-1].split(".")[0]
    powertrack_rules = generate_powertrack_rules.generate(expanded_users, ruleset_name)
    output_fname = os.path.join(OUTPUT_PATH, base_name + ".powertrack_rules.json")
    with open(output_fname, "w") as fs:
        print("\n".join([json.dumps(x) for x in powertrack_rules]),
              file=fs)
    logging.info("Powertrack rules in %s", output_fname)
