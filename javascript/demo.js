#!/usr/bin/env node

var _ = require('underscore')._;
var repubsub = require('./repubsub.js');

module.exports = {
    regexPublish: regexPublish,
    regexSubscribe: regexSubscribe,
    tagsPublish: tagsPublish,
    tagsSubscribe: tagsSubscribe,
    hierarchyPublish: hierarchyPublish,
    hierarchySubscribe: hierarchySubscribe,
    randomHierarchy: randomHierarchy,
}


// Publishes messages to a simple string topic
function regexPublish(){
    var exchange = new repubsub.Exchange('regex_demo', {db: 'repubsub'});

    setInterval(function(){
        var rnd = randomTopic();
        var topicKey = rnd.category + '.' + rnd.chartype + '.' + rnd.character;
        var payload = _.sample(CATEGORIES[rnd.category]);

        console.log('Publishing on topic ' + topicKey + ': ' + payload);

        exchange.topic(topicKey).publish(payload);
    }, 500);
}

// Subscribes to messages on a topic that match a regex
function regexSubscribe() {
    var exchange = new repubsub.Exchange('regex_demo', {db: 'repubsub'});
    var rnd = randomTopic();

    var maybeCharacter = _.sample([rnd.character, '(.+)']);
    var maybeChartype = _.sample([
        rnd.chartype + '\.' + maybeCharacter,
        '(.+)',
    ]);
    var topicRegex = '^' + rnd.category + '\.' + maybeChartype + '$';
    var queue = exchange.queue(function(topic){
        return topic.match(topicRegex);
    });
    
    var subMessage = "Subscribed to: " + topicRegex;
    printSubscription(subMessage);

    var i = 0;
    queue.subscribe(function(err, message){
        var topic = message.topic, payload = message.payload;
        if(i % 20 === 19){
            // Reminder what we're subscribed to
            printSubscription(subMessage);
        }
        console.log("Received on " + topic + ": " + payload);
        i++;
    });
}

// Publishes messages with an array of tags as a topic
function tagsPublish(){
    var exchange = new repubsub.Exchange('tags_demo', {db: 'repubsub'});

    setInterval(function(){
        // Get two random topics, remove duplicates, and sort them
        // Sorting ensures that if two topics consist of the same
        // tags, the same document in the database will be updated
        // This should result in 270 possible tag values
        var topicTags = _.union(_.values(randomTopic()),
                                _.values(randomTopic())).sort();
        var payload = _.sample(TEAMUPS.concat(EVENTS).concat(FIGHTS));
        
        console.log('Publishing on tags #' + topicTags.join(' #'));
        console.log('\t' + payload);

        exchange.topic(topicTags).publish(payload);
    }, 500);
}

// Subscribes to messages that have specific tags in the topic
function tagsSubscribe(){
    var exchange = new repubsub.Exchange('tags_demo', {db: 'repubsub'});

    var tags = _.sample(_.values(randomTopic()), 2);
    var queue = exchange.queue(function(topic){
        return topic.contains.apply(tags);
    });

    var subMessage = "Subscribed to messages with tags #" + tags.join(' #');
    printSubscription(subMessage);

    var i = 0;
    queue.subscribe(function(err, message){
        var topic = message.topic, payload = message.payload;
        if(i % 10 === 9){
            // Reminder what we're subscribed to 
            printSubscription(subMessage);
        }
        console.log("Received message with tags: #" + topic.join(' #'));
        console.log("\t" + payload);
        i++;
    });
}

// Publishes messages on a hierarchical topic
function hierarchyPublish(){
    var exchange = new repubsub.Exchange('hierarchy_demo', {db: 'repubsub'});

    setInterval(function(){
        var rnd = randomHierarchy();
        var topicObj = rnd.topicObj, payload = rnd.payload;

        console.log('Publishing on a hierarchical topic:');
        printHierarchy(rnd.topicObj);
        console.log(' -' + payload + '\n');

        exchange.topic(rnd.topicObj).publish(rnd.payload);
    }, 500);
}

// Subscribes to messages on a hierarchical topic
function hierarchySubscribe(){
    var exchange = new repubsub.Exchange('hierarchy_demo', {db: 'repubsub'});

    var rnd = randomTopic();
    var queue = exchange.queue(function(topic){
        return topic(rnd.category)(rnd.chartype).contains(rnd.character);
    });
    var subMessage = "Subscribed to topic('" + rnd.category + "')('" + 
            rnd.chartype + "').contains('" + rnd.character + "')";
    printSubscription(subMessage);

    var i = 0;
    queue.subscribe(function(err, message){
        if(i % 5 == 4){
            printSubscription(subMessage);
        }
        console.log('Received message with topic:');
        printHierarchy(message.topic);
        console.log(' -' + message.payload + '\n');
        i++;
    });
}

// Returns an object with the pieces of a random topic
function randomTopic(){
    var ret = {
        category: _.sample(_.keys(CATEGORIES)),
        chartype: _.sample(_.keys(CHARACTERS)),
    }
    ret.character = _.sample(CHARACTERS[ret.chartype]);

    return ret;
}

// Returns a random hierarchical topic
function randomHierarchy(){
    var topic = {};
    var categories = [];
    _.chain(CATEGORIES).keys().sample(_.random(1,2)).each(function(category){
        Array.prototype.push.apply(categories, CATEGORIES[category]);
        _.chain(CHARACTERS).keys().sample(_.random(1,2)).each(function(chartype){
            _.sample(CHARACTERS[chartype], _.random(1,2)).forEach(function(character){
                var cat = topic[category] || (topic[category] = {});
                var ct = cat[chartype] || (cat[chartype] = []);
                ct.push(character);
                ct.sort();
            });
        });
    });
    return {topicObj: topic, payload: _.sample(categories)}
}

// Prints a topic hierarchy nicely
function printHierarchy(obj){
    _.pairs(obj).forEach(function(ccPair){
        var category = ccPair[0], chartypes = ccPair[1];
        console.log('    ' + category);
        _.pairs(chartypes).forEach(function(ctPair){
            var charType = ctPair[0], characters = ctPair[1];
            console.log("        " + charType + ": " + characters.join(', '));
        });
    });
}

// Prints a subscription reminder message
function printSubscription(sub){
    console.log(new Array(sub.length + 1).join('='));
    console.log(sub);
    console.log(new Array(sub.length + 1).join('='));
    console.log();
}

// These are used in the demos

var CHARACTERS = {
    superheroes: ['Batman', 'Superman', 'CaptainAmerica'],
    supervillains: ['Joker', 'LexLuthor', 'RedSkull'],
    sidekicks: ['Robin', 'JimmyOlsen', 'BuckyBarnes'],
}

var TEAMUPS = [
    "You'll never guess who's teaming up",
    'A completely one-sided fight between superheroes',
    'Sidekick goes on rampage. Hundreds given parking tickets',
    'Local politician warns of pairing between villains',
    'Unexpected coalition teams up to take on opponents',
]

var FIGHTS = [
    'A fight rages between combatants',
    'Tussle between mighty foes continues',
    'All out war in the streets between battling heroes',
    "City's greatest hero defeated!",
    "Villain locked in minimum security prison after defeat",
]

var EVENTS = [
    "Scientists accidentally thaw a T-Rex and release it",
    "Time vortex opens over downtown",
    "EMP turns out the lights. You'll never guess who turned them back on",
    "Inter-dimensional sludge released. Who can contain it?",
    "Super computer-virus disables all police cars. City helpless.",
]

var CATEGORIES = {
    teamups: TEAMUPS,
    fights: FIGHTS,
    events: EVENTS,
}

function main(){
    var argv = require('yargs')
      .usage('$0 [-h] {regex,tags,hierarchy} {publish,subscribe}\n' +
             '\n' +
             'Demo for RethinkDB pub-sub\n' +
             '\n' +
             'positional arguments:\n' +
             '  {regex,tags,hierarchy}  Which demo to run\n' +
             '  {publish,subscribe}     Whether to publish or subscribe'
            )
      .demand(2)
      .check(function(argv){
          var demos = ['regex', 'tags', 'hierarchy'];
          var pubOrSub = ['publish', 'subscribe'];
          if (!_.contains(demos, argv._[0])){
              throw "First arg must be regex, tags or hierarchy";
          }
          if (!_.contains(pubOrSub, argv._[1])){
              throw "Second arg must publish or subscribe";
          }
          return true;
      })
      .argv;

    var demoName = argv._[0] + argv._[1].slice(0,1).toUpperCase() + argv._[1].slice(1);
    module.exports[demoName]();
}

main()
