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
        'demo',
        type=str,
        help='Which demo to run',
        choices=['regex', 'tags', 'hierarchy'],
    )
    parser.add_argument(
        'pub_or_sub',
        type=str,
        help="Whether to publish or subscribe",
        choices=['publish', 'subscribe'],
    )
    args = parser.parse_args()

    globals()['{0.demo}_{0.pub_or_sub}'.format(args)]()


def regex_publish():
    '''Publishes messages to a simple string topic'''

    exchange = repubsub.Exchange('regex_demo', db='repubsub')

    while True:
        category, chartype, character = random_topic()
        topic_key = '{category}.{chartype}.{character}'.format(
            category=category, chartype=chartype, character=character)
        payload = random.choice(CATEGORIES[category])

        print 'Publishing on topic', topic_key, ':', payload

        exchange.topic(topic_key).publish(payload)
        time.sleep(0.5)


def regex_subscribe():
    '''Subscribes to messages on a topic that match a regex'''

    exchange = repubsub.Exchange('regex_demo', db='repubsub')

    category, chartype, character = random_topic()
    topic_regex = r'^{category}\.{chartype_character}$'.format(
        # This avoids regexes like 'fights\.(.+)\.Batman' where the
        # chartype can only be one thing.
        chartype_character = random.choice([
            chartype + '\.' + random.choice([character, '(.+)']),
            '(.+)',
        ]),
        category = random.choice([category, '(.+)']),
    )
    reql_filter = lambda topic: topic.match(topic_regex)
    queue = exchange.queue(reql_filter)

    sub_message = 'Subscribed to: %s' % topic_regex
    print_subscription(sub_message)

    for i, (topic, payload) in enumerate(queue.subscription()):
        if i % 20 == 19:
            # Reminder what we're subscribed to
            print_subscription(sub_message)

        print 'Received on', topic, ':', payload


def tags_publish():
    '''Publishes messages with an array of tags as a topic'''

    exchange = repubsub.Exchange('tags_demo', db='repubsub')
    
    while True:
        # Get two random topics, remove duplicates, and sort them
        # Sorting ensures that if two topics consist of the same
        # tags, the same document in the database will be updated
        # This should result in 270 possible tag values
        topic_tags = sorted(set(random_topic() + random_topic()))
        payload = random.choice(TEAMUPS + EVENTS + FIGHTS)

        print 'Publishing on tags #{}'.format(' #'.join(topic_tags))
        print '\t', payload

        exchange.topic(topic_tags).publish(payload)
        time.sleep(0.5)


def tags_subscribe():
    '''Subscribes to messages that have specific tags in the topic'''

    exchange = repubsub.Exchange('tags_demo', db='repubsub')
    
    tags = random.sample(random_topic(), 2)
    reql_filter = lambda topic: topic.contains(*tags)
    queue = exchange.queue(reql_filter)

    sub_message = 'Subscribed to messages with tags: #%s' % ' #'.join(tags)
    print_subscription(sub_message)

    for i, (topic_tags, payload) in enumerate(queue.subscription()):
        if i % 10 == 9:
            # Reminder what we're subscribed to
            print_subscription(sub_message)

        print 'Received message with tags: #{}'.format(' #'.join(topic_tags))
        print '\t', payload
        print


def hierarchy_publish():
    '''Publishes messages on a hierarchical topic'''

    exchange = repubsub.Exchange('hierarchy_demo', db='repubsub')

    while True:
        topic_key, payload = random_hierarchy()

        print 'Publishing on hierarchical topic:'
        print_hierarchy(topic_key)
        print ' -', payload
        print

        exchange.topic(topic_key).publish(payload)
        time.sleep(0.5)


def hierarchy_subscribe():
    '''Subscribes to messages on a hierarchical topic'''

    exchange = repubsub.Exchange('hierarchy_demo', db='repubsub')

    category, chartype, character = random_topic()
    reql_filter = lambda topic: topic[category][chartype].contains(character)
    queue = exchange.queue(reql_filter)

    sub_message = 'Subscribed to topic: [%r][%r].contains(%r)' % (
        category, chartype, character)
    print_subscription(sub_message)

    for i, (topic, payload) in enumerate(queue.subscription()):
        if i % 5 == 4:
            # Reminder what we're subscribed to
            print_subscription(sub_message)

        print 'Received message with topic:'
        print_hierarchy(topic)
        print ' -', payload, '\n'


def random_topic():
    '''Returns the pieces of a random topic'''
    category = random.choice(CATEGORIES.keys())    
    chartype = random.choice(CHARACTERS.keys())
    character = random.choice(CHARACTERS[chartype])
    return category, chartype, character


def random_hierarchy():
    '''Returns a random hierarchical topic'''
    topic = {}
    categories = []
    for category in random.sample(CATEGORIES.keys(), randint(1, 2)):
        categories.extend(CATEGORIES[category])
        for chartype in random.sample(CHARACTERS.keys(), randint(1, 2)):
            for character in random.sample(CHARACTERS[chartype], randint(1, 2)):
                characters = topic.setdefault(
                    category, {}).setdefault(chartype, [])
                characters.append(character)
                characters.sort()
    return topic, random.choice(categories)


def print_hierarchy(h):
    '''Prints a topic hierarchy nicely'''
    for category, chartypes in h.iteritems():
        print '   ', category, ':'
        for chartype, characters in chartypes.iteritems():
            print '       ', chartype, ':', ', '.join(characters)

def print_subscription(sub):
    '''Prints a subscription reminder message'''
    print '=' * len(sub)
    print sub
    print '=' * len(sub)
    print


# These are used in the demos
CHARACTERS = {
    'superheroes': ['Batman', 'Superman', 'CaptainAmerica'],
    'supervillains': ['Joker', 'Lex Luthor', 'RedSkull'],
    'sidekicks': ['Robin', 'JimmyOlsen', 'BuckyBarnes'],
}

TEAMUPS = [
    "You'll never guess who's teaming up",
    'A completely one-sided fight between superheroes',
    'Sidekick goes on rampage. Hundreds given parking tickets',
    'Local politician warns of pairing between villains',
    'Unexpected coalition teams up to take on opponents',
]

FIGHTS = [
    'A fight rages between combatants',
    'Tussle between mighty foes continues',
    'All out war in the streets between battling heroes',
    "City's greatest hero defeated!",
    "Villain locked in minimum security prison after defeat",
]

EVENTS = [
    "Scientists accidentally thaw a T-Rex and release it",
    "Time vortex opens over downtown",
    "EMP turns out the lights. You'll never guess who turned them back on",
    "Inter-dimensional sludge released. Who can contain it?",
    "Super computer-virus disables all police cars. City helpless.",
]

CATEGORIES = {
    'teamups': TEAMUPS,
    'fights': FIGHTS,
    'events': EVENTS,
}

if __name__ == '__main__':
    main()
