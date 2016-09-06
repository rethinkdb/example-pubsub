# repubsub.py #

Repubsub is a publish-subscribe library built on top of
[RethinkDB](http://rethinkdb.com). This is the python version of
the library. There is a
[full article](http://rethinkdb.com/docs/publish-subscribe/python/)
describing this library in depth.

## Installation ##

You'll need to install RethinkDB first. You can find instructions for
that [on this page](http://rethinkdb.com/docs/install).

To install the library, go into the source directory containing
`setup.py` and run:

```bash
$ python setup.py install
```

## Usage ##

To connect to an exchange, create a topic and publish to it:

```python
import repubsub

exchange = repubsub.Exchange('exchange_name', db='database_name')

topic = exchange.topic('hi.there')

topic.publish("All subscribers to 'hi.there' will pick this up")
```

To create a queue for listening for messages, the process is similar
except you'll need to create a
[ReQL](http://rethinkdb.com/docs/introduction-to-reql/) filter
function:

```python
queue = exchange.queue(lambda topic: topic.match('hi.*'))

for topic, message in queue.subscription():
    print 'Received the message:', message
    print 'on the topic:', topic
```

In addition, examples of usage can be found in the [demo.py](https://github.com/rethinkdb/example-pubsub/blob/master/python/demo.py)
file. There is also an extensive description of how the library works
and how to use it
[here](http://rethinkdb.com/docs/publish-subscribe/python).

## Bugs ##

Please report any bugs at our
[github issue tracker](https://github.com/rethinkdb/example-pubsub/issues)
