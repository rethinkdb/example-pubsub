#!/usr/bin/env python2
'''Implementation of message queueing on top of RethinkDB changefeeds.

In this model, exchanges are databases, and documents are topics. The
current value of the topic in the database is just whatever the last
message sent happened to be. The document only exists to force
RethinkDB to generate change notifications for changefeed
subscribers. These notifications are the actual messages.

Internally, RethinkDB buffers changefeed notifications in a buffer per
client connection. These buffers are analogous to AMQP queues. This
has several benefits vs. (for example) having one document per
message:

* change notifications aren't created unless someone is subscribed
* notifications are deleted as soon as they're read from the buffer
* the notification buffers are implicitly ordered, so no sorting needs
  to happen at the query level.

One large difference from existing message queues like RabbitMQ is
that there is no way to cause the change buffers to be persisted
across connections. Because of this, if the client sends a STOP
request to the changefeed, or disconnects, the queue is effectively
lost. Any messages on the queue are unrecoverable.

'''

from functools import wraps
import uuid
import logging

import rethinkdb as r

LOGGER = logging.getLogger('rethinkmq')

def binding_key_to_regex(pattern):
    '''Takes an AMQP binding key and translates it to re2 regex'''
    sections = []
    for section in pattern.split('.'):
        if section == '*':
            # match exactly one word
            sections.append(r'\.[a-zA-Z]+')
        elif section == '#':
            # match zero or more words separated by periods
            sections.append(r'(?:\.[a-zA-Z]+)*')
        else:
            # just match the word directly
            sections.append(r'\.' + section)
    return '^{}$'.format(''.join(sections).lstrip(r'\.'))


def assert_table(meth):
    '''Decorator for the Exchange class that ensures a table exists before
    running the method.'''
    @wraps(meth)
    def _wrapper(self, *args, **kwargs):
        if not self._asserted:
            self.conn.assert_table(self.name)
            self._asserted = True
        return meth(self, *args, **kwargs)
    return _wrapper


class Exchange(object):
    '''Represents a message exchange which messages can be sent to and
    consumed from. Each exchange has an underlying RethinkDB table.'''

    def __init__(self, conn, name='messages'):
        self.name = name
        self.conn = conn
        self.table = r.table(name)
        self._asserted = False

    def topic(self, name):
        '''Returns a topic in this exchange'''
        return Topic(name, self)

    def queue(self, *patterns):
        '''Returns a new queue on this exchange (with no bindings)'''
        return Queue(self, *patterns)

    @assert_table
    def publish(self, topic, payload):
        '''Publish a message to this exchange on the given topic'''
        self.conn(self.table.insert({
            'topic': topic,
            'payload': payload,
            '_force_change': str(uuid.uuid4()),
        }, upsert=True))

    @assert_table
    def consume(self, *patterns):
        '''Generator of messages from the exchange with topics matching the
        given binding patterns
        '''
        if not patterns:
            raise Exception('Nothing to consume, no patterns provided')
        LOGGER.info(
            'Listening for patterns: %s', ', '.join(repr(p) for p in patterns))
        regexes = [binding_key_to_regex(pattern) for pattern in patterns]
        matchers = r.row['topic'].match(regexes.pop(0))
        for regex in regexes:
            matchers |= r.row['topic'].match(regex)

        filter_clause = r.row != None and matchers
        for message in self.conn(self.table.changes()['new_val']
                                 .filter(filter_clause)):
            yield message['topic'], message['payload']


class Topic(object):
    '''Represents a heirarchical topic on an exchange. Underlying
    every topic is a single document in the exchange's database.'''

    def __init__(self, name, exchange):
        self.name = name
        self.exchange = exchange

    def __getitem__(self, subtopic_name):
        '''Returns a new subtopic from the current one.'''
        subtopic_name = self.name + '.' + subtopic_name
        return Topic(subtopic_name, self.exchange)

    def publish(self, payload):
        '''Publish a payload to the current topic'''
        self.exchange.publish(self.name, payload)

    def queue(self):
        '''Returns a new queue that listens on this topic'''
        return Queue(self.exchange, self)


class Queue(object):
    '''Listens for different topics. Represents a change buffer on the
    server'''

    def __init__(self, exchange, *patterns):
        # unlike RabbitMQ, we can't name queues. But we don't need to
        # anyway, since if you lose your connection to the server or
        # stop consuming, the buffer is deleted and you have to create
        # a new one
        self.exchange = exchange
        self._bindings = []
        self.bind(*patterns)

    def bind(self, *patterns):
        '''Bind the current queue to the given pattern. May also pass in a
        topic as a pattern'''
        LOGGER.debug(
            'binding patterns: %s',', '.join(repr(p) for p in patterns))
        for pattern in patterns:
            if isinstance(pattern, Topic):
                self._bindings.append(pattern.name + '.#')
            else:
                self._bindings.append(pattern)

    def consume(self):
        '''Returns a generator that returns messages from this queue's
        subscriptions'''
        return self.exchange.consume(*self._bindings)


class MQConnection(object):
    '''Represents a connection with some MQ-specific behavior. Each
    MQConnection has an associated database, but multiple connections
    can be made to the same database.
    '''
    def __init__(self,
                 db='MQ',
                 host='localhost',
                 port=28015,
                 password=None):
        self._conn = r.connect(host, port, password)
        self._db = db
        self._conn.use(db)

    def exchange(self, name='messages'):
        '''Returns an exchange for the current connection'''
        return Exchange(self, name)

    def assert_table(self, name):
        '''Ensures the table specified exists and has the correct
        primary_key and durability settings'''
        try:
            # We set durability to soft because we don't actually care
            # if the write is confirmed on disk, we just want the
            # change notification to be generated.
            self(r.table_create(name, primary_key='topic', durability='soft'))
            LOGGER.info('Created table %r in database %s', name, self._db)
        except r.RqlRuntimeError as rre:
            if 'already exists' not in rre.message:
                raise
            LOGGER.debug('Table %r already exists, moving on', name)

    def __call__(self, query, **kwargs):
        '''Run a query with this connection'''
        LOGGER.debug('Running: %s', query)
        return query.run(self._conn, **kwargs)
