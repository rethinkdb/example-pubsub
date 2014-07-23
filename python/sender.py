#!/usr/bin/env python2
'''This is a simple message sender application that uses the rethinkmq
library.

It receives a message at the command line and sends it to the
specified topic
'''

import sys
import logging

from rethinkmq import MQConnection

USAGE = '''\
Usage: {0} [-v] <topic> <message>

Examples:

  Broadcast the temperature in Mountain View
    {0} weather.us.ca.mountainview 74
  Broadcast the temperature in Mountain View
    {0} weather.uk.conditions "A bit rainy"

'''

def main(argv):
    if len(argv) < 3:
        print USAGE.format(argv[0])
        return

    if argv[1] == '-v':
        logging.root.setLevel(logging.DEBUG)
        logging.basicConfig()
        argv = argv[1:]

    # Publish on the topic (will create the table if necessary)
    MQConnection(db='MQ').exchange('messages').topic(argv[1]).publish(argv[2])


if __name__ == '__main__':
    main(sys.argv)
