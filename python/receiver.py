#!/usr/bin/env python2
'''This is a simple example receiver that uses the rethinkmq library.

It simply binds a queue to several patterns and then consumes the
queue forever, printing out any messages received.
'''

import sys
import logging

from rethinkmq import MQConnection


USAGE = '''\
Usage: {0} <topic_pattern> [<topic_pattern>...]

Examples:

  Get all messages for temperature topics in the US or UK
    {0} weather.us.temp weather.uk.temp

  Get all messages for any state in the US named Springfield
    {0} 'weather.us.*.springfield'

  Get all temperature messages no matter where they're from
    {0} 'weather.#.temp'
'''

def main(argv):
    if not argv[1:]:
        print USAGE.format(argv[0])
        return

    if argv[1] == '-v':
        logging.root.setLevel(logging.DEBUG)
        logging.basicConfig()
        argv = argv[1:]


    # create the queue (no tables or documents are created yet)
    queue = MQConnection('MQ').exchange('messages').queue(*argv[1:])

    # Listen on the queue (this lazily creates the table if necessary)
    for topic, payload in queue.consume():
        print '[{}] received:'.format(topic)
        print '   ', payload
    

if __name__ == '__main__':
    main(sys.argv)
