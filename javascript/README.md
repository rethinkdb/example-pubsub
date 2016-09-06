# repubsub.js #

Repubsub is a publish-subscribe library built on top of
[RethinkDB](http://rethinkdb.com). This is the javascript version of
the library. There is a
[full article](http://rethinkdb.com/docs/publish-subscribe/javascript/)
describing this library in depth.

## Installation ##

You'll need to install RethinkDB first. You can find instructions for
that [on this page](http://rethinkdb.com/docs/install).

To install the library, go into the source directory containing
`package.json` and run:

```bash
$ npm install
```

## Usage ##

To connect to an exchange, create a topic and publish to it:

```javascript
var repubsub = require('repubsub');

var exchange = new repubsub.Exchange('exchangeName', {db: 'databaseName'});

var topic = exchange.topic('hi.there');

topic.publish("All subscribers to 'hi.there' will pick this up");
```

To create a queue for listening for messages, the process is similar
except you'll need to create a
[ReQL](http://rethinkdb.com/docs/introduction-to-reql/) filter
function:

```javascript
function topicFilter(topic){
    return topic.match('hi.*');
}
var queue = exchange.queue(topicFilter);

queue.subscribe(function(topic, message){
    console.log('Received the message:', message);
    console.log('on the topic:', topic);
});
```

In addition, examples of usage can be found in the
[demo.js](https://github.com/rethinkdb/example-pubsub/blob/master/javascript/demo.js)
file. There is also an extensive description of how the library works
and how to use it
[here](http://rethinkdb.com/docs/publish-subscribe/javascript).

## Bugs ##

Please report any bugs at our
[github issue tracker](https://github.com/rethinkdb/example-pubsub/issues)
