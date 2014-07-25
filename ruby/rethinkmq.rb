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


require 'logger'
require 'securerandom'
require 'rethinkdb'
include RethinkDB::Shortcuts


$RMQLogger = Logger.new(STDOUT)


class String
  # Takes an AMQP binding key and translates it to re2 regex, which
  # RethinkDB's 'match' function accepts
  def binding_key_to_regex
    sections = self.split('.').map do |section|
      if section == '*'
        '\.[a-zA-Z]+'
      elsif section == '#'
        '(?:\.[a-zA-Z]+)*'
      else
        '\.' + section
      end
    end
    sections.join().sub("\\.", "")
  end
end

# Represents a connection with some MQ-specific behavior. Each
# MQConnection has an associated database, but multiple connections
# can be made to the same database.

class MQConnection < RethinkDB::Connection

  def initialize(db='MQ', host='localhost', port=28015, auth_key='')
    super(:db => db, :host => host, :port => port, :auth_key => auth_key)
  end

  # Returns an exchange for the current connection
  def exchange(name='messages')
    Exchange.new(self, name)
  end
end


# Represents a message exchange which messages can be sent to and
# consumed from. Each exchange has an underlying RethinkDB table.

class Exchange
  def initialize(conn, name='messages')
    @name = name
    @conn = conn
    @table = r.table(name)
    @asserted = false
  end

  # Returns a topic in this exchange
  def topic(name)
    Topic.new(name, self)
  end

  # Returns a new queue on this exchange (with no bindings)
  def queue(*patterns)
    Queue.new(self, *patterns)
  end

  # Publish a message to this exchange on the given topic
  def publish(topic, payload)
    assert_table
    @table.insert({topic: topic,
                    payload: payload,
                    _force_change: SecureRandom.uuid},
                  :upsert => true).run(@conn)
  end

  # Runs its block for each messages from the exchange with topics
  # matching the given binding patterns. The block must accept two
  # arguments: topic and payload
  def consume(*patterns, &block)
    assert_table
    $RMQLogger.info("Listening for patterns: #{patterns}")
    query = query_from_patterns(patterns)
    query.run(@conn).each do |message|
      yield message['topic'], message['payload']
    end
  end

  private
    # Convert a list of regexes into a ReQL clause filtering a
    # changefeed on the current Exchange's table
    def query_from_patterns(patterns)
      @table.changes()['new_val'].filter{|row|
        binding = row.ne(nil)
        if not patterns.empty?
          regexes = patterns.map(&:binding_key_to_regex)
          matchers = row['topic'].match(regexes.shift)
          regexes.each do |regex|
            matchers &= row['topic'].match(regex)
          end

          binding &= matchers
        end
        binding
      }
    end

    # Ensures the table specified exists and has the correct primary_key
    # and durability settings
    def assert_table
      if @asserted
        return
      end
      begin
        # We set durability to soft because we don't actually care if
        # the write is confirmed on disk, we just want the change
        # notification (i.e. the message on the queue) to be generated.
        r.table_create(@name,
                       :primary_key => 'topic',
                       :durability => 'soft').run(@conn)
        $RMQLogger.info(
            "Created table '#{@name}'")
      rescue RethinkDB::RqlRuntimeError => rre
        if not rre.to_s.include?("already exists")
          raise
        else
          $RMQLogger.debug("Table '#{@name}' already exists, moving on")
        end
      end
      @asserted = true
    end
end


# Represents a heirarchical topic on an exchange. Underlying every
# topic is a single document in the exchange's database.

class Topic
  def initialize(name, exchange)
    @name = name
    @exchange = exchange
  end

  # Returns a new subtopic from the current one
  def [](subtopic_name)
    Topic("#{@name}.#{subtopic_name}", @exchange)
  end

  # Publish a payload to the current topic
  def publish(payload)
    @exchange.publish(@name, payload)
  end
end

# Listens for different topics. Represents a change buffer on the server
class Queue
  def initialize(exchange, *patterns)
    # unlike RabbitMQ, we can't name queues. But we don't need to
    # anyway, since if you lose your connection to the server or stop
    # consuming, the buffer is deleted and you have to create a new
    # one
    @exchange = exchange
    @bindings = []
    bind(*patterns)
  end

  # Bind the current queue to the given pattern. May also pass in a
  # topic as a pattern
  def bind(*patterns)
    if not patterns.empty?
      $RMQLogger.debug("Binding patterns: #{patterns}")
      @bindings.push(*patterns)
    end
  end

  # Executes its block for every message received on this queue
  def consume(&block)
    @exchange.consume(*@bindings, &block)
  end
end
