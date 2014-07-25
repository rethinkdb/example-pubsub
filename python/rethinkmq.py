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


class MQConnection(r.Connection):
    '''A RethinkDB connection that facilitates exchange creation'''

    def __init__(self,
                 db='MQ',
                 host='localhost',
                 port=28015,
                 auth_key='',
                 timeout=20):
        super(MQConnection, self).__init__(host, port, db, auth_key, timeout)

    def exchange(self, name='messages'):
        '''Returns an exchange for the current connection'''
        return Exchange(self, name)


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

    def publish(self, topic, payload):
        '''Publish a message to this exchange on the given topic'''
        self._assert_table()
        self.table.insert({
            'topic': topic,
            'payload': payload,
            '_force_change': str(uuid.uuid4()),
        }, upsert=True).run(self.conn)


    def consume(self, *patterns):
        '''Generator of messages from the exchange with topics matching the
        given binding patterns
        '''
        self._assert_table()
        LOGGER.info('Listening for patterns: %r', patterns)
        query = self._query_from_patterns(patterns)

        for message in query.run(self.conn):
            yield message['topic'], message['payload']

    def _query_from_patterns(self, patterns):
        '''Convert a list of regexes into a ReQL clause filtering a
        changefeed on the current Exchange's table'''
        binding = r.row != None
        if patterns:
            regexes = map(binding_key_to_regex, patterns)
            matchers = r.row['topic'].match(regexes.pop(0))
            for regex in regexes:
                matchers |= r.row['topic'].match(regex)
            binding &= matchers
        return self.table.changes()['new_val'].filter(binding)

    def _assert_table(self):
        '''Ensures the table specified exists and has the correct
        primary_key and durability settings'''
        if self._asserted:
            return
        try:
            # We set durability to soft because we don't actually care
            # if the write is confirmed on disk, we just want the
            # change notification (i.e. the message on the queue) to
            # be generated.
            r.table_create(
                self.name,
                primary_key='topic',
                durability='soft',
            ).run(self.conn)
            LOGGER.info('Created table %r in database %s', name, self.conn.db)
        except r.RqlRuntimeError as rre:
            if 'already exists' not in rre.message:
                raise
            LOGGER.debug('Table %r already exists, moving on', self.name)
        self._asserted = True


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
        LOGGER.debug('binding patterns: %s', patterns)
        self._bindings.extend(patterns)

    def consume(self):
        '''Returns a generator that returns messages from this queue's
        subscriptions'''
        return self.exchange.consume(*self._bindings)
