'''Implementation of publish/subscribe on top of RethinkDB changefeeds.

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

import random

from rethinkdb import connect
import rethinkdb as r


__all__ = ['connect', 'Exchange', 'Topic', 'Queue']


class Exchange(object):
    '''Represents a message exchange which messages can be sent to and
    consumed from. Each exchange has an underlying RethinkDB table.'''

    def __init__(self, conn, name):
        self.name = name
        self.conn = conn
        self.table = r.table(name)
        self._asserted = False

    def __repr__(self):
        return 'Exchange({.name})'.format(self)

    def topic(self, topic_key):
        '''Returns a topic in this exchange with the given key'''
        return Topic(self, topic_key)

    def queue(self, binding_query=None):
        '''Returns a new queue on this exchange that will filter messages by
        the given query.'''
        return Queue(self, binding_query)

    def full_query(self, filter_func):
        '''Returns the full ReQL query for a given filter function'''
        return self.table.changes()['new_val'].filter(
            lambda row: filter_func(row['topic']))

    def publish(self, topic_key, payload):
        '''Publish a message to this exchange on the given topic'''
        self.assert_table()

        # first try to just update an existing document
        result = self.table.filter({
            'topic': topic_key
            if not isinstance(topic_key, dict)
            else r.literal(topic_key),
        }).update({
            'payload': payload,
            '_force_change': random.random(),
        }).run(self.conn)

        # If the topic doesn't exist yet, insert a new document. Note:
        # it's possible someone else could have inserted it in the
        # meantime and this would create a duplicate. That's a risk we
        # take here. The consequence is that duplicated messages may
        # be sent to the consumer.
        if not result['replaced']:
            result = self.table.insert({
                'topic': topic_key,
                'payload': payload,
                '_force_change': random.random(),
            }).run(self.conn)

    def subscribe(self, filter_func):
        '''Generator of messages from the exchange with topics matching the
        given filter function
        '''
        self.assert_table()

        for message in self.full_query(filter_func).run(self.conn):
            yield message['topic'], message['payload']

    def assert_table(self):
        '''Ensures the table specified exists and has the correct
        primary_key and durability settings'''
        if self._asserted:
            return
        try:
            # Assert the db into existence
            r.db_create(self.conn.db).run(self.conn)
        except r.RqlRuntimeError as rre:
            if 'already exists' not in rre.message:
                raise
        try:
            # We set durability to soft because we don't actually care
            # if the write is confirmed on disk, we just want the
            # change notification (i.e. the message on the queue) to
            # be generated.
            r.table_create(
                self.name,
                durability='soft',
            ).run(self.conn)
        except r.RqlRuntimeError as rre:
            if 'already exists' not in rre.message:
                raise
        self._asserted = True


class Topic(object):
    '''Represents a topic that may be published to.

    Also responds to any ReQL methods that can be used on
    `r.row`.
    '''

    def __init__(self, exchange, topic_key=None):
        self.key = topic_key
        self.exchange = exchange

    def publish(self, payload):
        '''Publish a payload to the current topic'''
        self.exchange.publish(self.key, payload)

    def __repr__(self):
        return 'Topic({})'.format(self.key)


class Queue(object):
    '''A queue that filters for messages in the exchange'''

    def __init__(self, exchange, filter_func):
        self.exchange = exchange
        self.filter_func = filter_func

    def subscribe(self):
        '''Returns a generator that returns messages from this queue's
        subscriptions'''
        return self.exchange.subscribe(self.filter_func)

    def full_query(self):
        '''Returns the full ReQL query for this queue'''
        return self.exchange.full_query(self.filter_func)

    def __repr__(self):
        return 'Queue({})'.format(r.expr(self.filter_func))
