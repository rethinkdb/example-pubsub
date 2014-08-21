# Implementation of message queueing on top of RethinkDB changefeeds.

# In this model, exchanges are databases, and documents are topics. The
# current value of the topic in the database is just whatever the last
# message sent happened to be. The document only exists to force
# RethinkDB to generate change notifications for changefeed
# subscribers. These notifications are the actual messages.

# Internally, RethinkDB buffers changefeed notifications in a buffer per
# client connection. These buffers are analogous to AMQP queues. This
# has several benefits vs. (for example) having one document per
# message:

# * change notifications aren't created unless someone is subscribed
# * notifications are deleted as soon as they're read from the buffer
# * the notification buffers are implicitly ordered, so no sorting needs
#   to happen at the query level.

# One large difference from existing message queues like RabbitMQ is
# that there is no way to cause the change buffers to be persisted
# across connections. Because of this, if the client sends a STOP
# request to the changefeed, or disconnects, the queue is effectively
# lost. Any messages on the queue are unrecoverable.

require 'set'

require 'rethinkdb'
include RethinkDB::Shortcuts

module Repubsub

  # Represents a message exchange which messages can be sent to and
  # consumed from. Each exchange has an underlying RethinkDB table.
  class Exchange
    def initialize(name, opts={})
      @db = opts.fetch(:db, :test)
      @name = name.to_s
      @conn = r.connect(opts)
      @table = r.table(name)
      @asserted = false
    end

    # Returns a topic in this exchange
    def topic(name)
      Topic.new(self, name)
    end

    # Returns a new queue on this exchange that will filter messages
    # by the given query
    def queue(&filter_func)
      Queue.new(self, &filter_func)
    end

    # The full ReQL query for a given filter function
    def full_query(filter_func)
      @table.changes[:new_val].filter{|row| filter_func.call(row[:topic])}
    end

    # Publish a message to this exchange on the given topic
    def publish(topic_key, payload)
      assert_table

      result = @table.filter(
        :topic => topic_key.class == Hash ? r.literal(topic_key) : topic_key
      ).update(
        :payload => payload,
        :updated_on => r.now,
      ).run(@conn)

      # If the topic doesn't exist yet, insert a new document. Note:
      # it's possible someone else could have inserted it in the
      # meantime and this would create a duplicate. That's a risk we
      # take here. The consequence is that duplicated messages may
      # be sent to the consumer.
      if result['replaced'].zero?
        @table.insert(
          :topic => topic_key,
          :payload => payload,
          :updated_on => r.now,
        ).run(@conn)
      end
    end

    # Lazy Enumerator of messages from the exchange with topics
    # matching the given filter
    def subscription(filter_func)
      assert_table
      full_query(filter_func).run(@conn).lazy.map do |message|
        [message['topic'], message['payload']]
      end
    end

    # Ensures the table specified exists and has the correct primary_key
    # and durability settings
    def assert_table
      if @asserted
        return
      end
      begin
        # Assert the db into existence
        r.db_create(@db).run(@conn)
      rescue RethinkDB::RqlRuntimeError => rre
        unless rre.to_s.include?('already exists')
          raise
        end
      end
      begin
        # We set durability to soft because we don't actually care if
        # the write is confirmed on disk, we just want the change
        # notification (i.e. the message on the queue) to be generated.
        r.table_create(@name, :durability => :soft).run(@conn)
      rescue RethinkDB::RqlRuntimeError => rre
        unless rre.to_s.include?('already exists')
          raise
        end
      end
      @asserted = true
    end
  end


  # Represents a topic that may be published to.

  class Topic
    def initialize(exchange, topic_key)
      @key = topic_key
      @exchange = exchange
    end

    # Publish a payload to the current topic
    def publish(payload)
      @exchange.publish(@key, payload)
    end
  end

  # A queue that filters messages in the exchange
  class Queue
    def initialize(exchange, &filter_func)
      @exchange = exchange
      @filter_func = filter_func
    end

    # Returns the full ReQL query for this queue
    def full_query
      @exchange.full_query(@filter_func)
    end

    # An Enumerator of messages from this queue's subscriptions
    def subscription
      @exchange.subscription(@filter_func)
    end
  end

end
