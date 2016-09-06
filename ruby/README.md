# repubsub.rb #

Repubsub is a publish-subscribe library built on top of
[RethinkDB](http://rethinkdb.com). This is the ruby version of
the library. There is a
[full article](http://rethinkdb.com/docs/publish-subscribe/ruby/)
describing this library in depth.

## Installation ##

You'll need to install RethinkDB first. You can find instructions for
that [on this page](http://rethinkdb.com/docs/install).

To install the library, go into the source directory containing
`repubsub.gemspec` and run:

```bash
$ bundle install
```

## Usage ##

To connect to an exchange, create a topic and publish to it:

```ruby
require 'repubsub'

exchange = Repubsub::Exchange.new(:exchange_name, :db => :database_name)

topic = exchange.topic('hi.there')

topic.publish("All subscribers to 'hi.there' will pick this up")
```

To create a queue for listening for messages, the process is similar
except you'll need to provide a
[ReQL](http://rethinkdb.com/docs/introduction-to-reql/) filter
block:

```ruby
queue = exchange.queue{|topic| topic.match('hi.*')}

queue.subscription.each do |topic, message|
    puts "Received the message #{message}"
    puts "on the topic: #{topic}"
end
```

In addition, examples of usage can be found in the
[demo.rb](https://github.com/rethinkdb/example-pubsub/blob/master/ruby/demo.rb)
file. There is also an extensive description of how the library works
and how to use it [here](http://rethinkdb.com/docs/publish-subscribe/ruby).

## Bugs ##

Please report any bugs at our
[github issue tracker](https://github.com/rethinkdb/example-pubsub/issues)
