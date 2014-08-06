#!/usr/bin/env python2

import sys
import logging
import argparse
import time
import random
from random import randint

import repubsub
import rethinkdb as r


def main():
    '''Parse command line args and use them to run the corresponding
    function'''
    parser = argparse.ArgumentParser(
        description='Demo for RethinkDB pub-sub')
    parser.add_argument(
        'pub_or_sub',
        type=str,
        help="Whether to publish or subscribe",
        choices=['publish', 'subscribe'],
    )
    parser.add_argument(
        'demo',
        type=str,
        help='Which demo to run',
        choices=['regex', 'tags', 'hierarchy'],
    )
    args = parser.parse_args()

    globals()['{0.demo}_{0.pub_or_sub}'.format(args)]()


def regex_publish():
    '''Publishes messages to a simple string topic'''

    conn = r.connect(db='repubsub')
    exchange = repubsub.Exchange(conn, 'regex_demo')

    while True:
        category, state, city = random_topic()
        topic_key = '{category}.{state}.{city}'.format(
            category=category, state=state, city=city)
        payload = random.choice(CATEGORIES[category])

        print 'Publishing on topic', topic_key, ':', payload

        exchange.topic(topic_key).publish(payload)
        time.sleep(0.5)


def regex_subscribe():
    '''Subscribes to messages on a topic that match a regex'''

    conn = r.connect(db='repubsub')
    exchange = repubsub.Exchange(conn, 'regex_demo')

    category, state, city = random_topic()
    topic_regex = r'^{category}\.{state_city}$'.format(
        # This avoids regexes like 'traffic\.(.+)\.salem' where the
        # state can only be one thing.
        state_city = random.choice([
            state + '.' + random.choice([city, '(.+)']),
            '(.+)',
        ]),
        category = random.choice([category, '(.+)']),
    )
    reql_query = r.row['topic'].match(topic_regex)
    queue = exchange.queue(reql_query)
    
    def print_subscription():
        print '=' * 20
        print 'Subscribed to:', topic_regex
        print '=' * 20

    print_subscription()

    for i, (topic, payload) in enumerate(queue.consume()):
        if i % 20 == 19:
            # Reminder what we're subscribed to
            print_subscription()

        print 'Received on', topic, ':', payload


def tags_publish():
    '''Publishes messages with an array of tags as a topic'''

    conn = r.connect(db='repubsub')
    exchange = repubsub.Exchange(conn, 'tags_demo')
    
    while True:
        # Get two random topics, remove duplicates, and sort them
        # Sorting ensures that if two topics consist of the same
        # tags, the same document in the database will be updated
        # This should result in 270 possible tag values
        topic_key = sorted(set(random_topic() + random_topic()))
        payload = random.choice(
            NEWS_STORIES + TRAFFIC_STORIES + WEATHER_STORIES)

        print 'Publishing on tags [', ', '.join(topic_key), ']:'
        print '\t', payload

        exchange.topic(topic_key).publish(payload)
        time.sleep(0.5)


def tags_subscribe():
    '''Subscribes to messages that have specific tags in the topic'''

    conn = r.connect(db='repubsub')
    exchange = repubsub.Exchange(conn, 'tags_demo')
    
    tags = random.sample(random_topic(), 2)
    reql_query = r.row['topic'].contains(*tags)
    queue = exchange.queue(reql_query)

    def print_subscription():
        print '=' * 20
        print 'Subscribed to messages with tags: [', ', '.join(tags), ']'
        print '=' * 20

    print_subscription()

    for i, (topic, payload) in enumerate(queue.consume()):
        if i % 10 == 9:
            # Reminder what we're subscribed to
            print_subscription()

        print 'Received message with tags: [', ', '.join(topic), ']'
        print '\t', payload


def hierarchy_publish():
    '''Publishes messages on a hierarchical topic'''

    conn = r.connect(db='repubsub')
    exchange = repubsub.Exchange(conn, 'hierarchy_demo')

    while True:
        topic_key = random_hierarchy()
        payload = random.choice(
            NEWS_STORIES + TRAFFIC_STORIES + WEATHER_STORIES)

        print 'Publishing on hierarchical topic:'
        print_hierarchy(topic_key)
        print ' -', payload
        print

        exchange.topic(topic_key).publish(payload)
        time.sleep(0.5)


def hierarchy_subscribe():
    '''Subscribes to messages on a hierarchical topic'''

    conn = r.connect(db='repubsub')
    exchange = repubsub.Exchange(conn, 'hierarchy_demo')

    category, state, city = random_topic()
    reql_query = r.row['topic'][category][state].contains(city)
    queue = exchange.queue(reql_query)

    def print_subscription():
        print '=' * 20
        print 'Subscribed to topic: ',
        print '[{category!r}][{state!r}].contains({city!r})'.format(
            category=category, state=state, city=city)
        print '=' * 20

    print_subscription()
    for i, (topic, payload) in enumerate(queue.consume()):
        if i % 5 == 4:
            # Reminder what we're subscribed to
            print_subscription()

        print 'Received message with topic:'
        print_hierarchy(topic)
        print ' -', payload, '\n'


def random_topic():
    '''Returns the pieces of a random topic'''
    category = random.choice(CATEGORIES.keys())    
    state = random.choice(LOCATIONS.keys())
    city = random.choice(LOCATIONS[state])
    return category, state, city


def random_hierarchy():
    '''Returns a random hierarchical topic'''
    ret = {}
    for category in random.sample(CATEGORIES.keys(), randint(1, 3)):
        for state in random.sample(LOCATIONS.keys(), randint(1, 2)):
            for city in random.sample(LOCATIONS[state], randint(1, 2)):
                ret.setdefault(category, {}) \
                   .setdefault(state, []) \
                   .append(city)
    return ret


def print_hierarchy(h):
    '''Prints a topic hierarchy nicely'''
    for category, states in h.iteritems():
        print '   ', category, ':'
        for state, cities in states.iteritems():
            print '       ', state, ':', ', '.join(cities)


# These are used in the demos
LOCATIONS = {
    'fl': ['tampa', 'miami', 'orlando'],
    'ca': ['mountainview', 'cupertino', 'sanjose'],
    'ma': ['boston', 'cambridge', 'salem'],
}

NEWS_STORIES = [
    'Person in dinosaur suit robs a grocery store',
    'New study says nobody really washes their hands',
    'Tall buildings scary claims local man',
    'Local politician found not to be corrupt',
    'Common ingredient in kitchens killing you right now',
]

WEATHER_STORIES = [
    "It's hot.",
    "It's cold.",
    "It's rainy.",
    "It's foggy.",
    "It's pretty nice.",
    "Tsunami warning",
    "Hurricane warning",
    "Blizzard warning",
]

TRAFFIC_STORIES = [
    "Congestion on major roads",
    "Couch fell off truck on the highway",
    "Accident near downtown intersection",
    'Fifty car pile-up',
    'Rush-hour',
    'People playing frisbee on the highway',
]

CATEGORIES = {
    'news': NEWS_STORIES,
    'weather': WEATHER_STORIES,
    'traffic': TRAFFIC_STORIES,
}

if __name__ == '__main__':
    main()
