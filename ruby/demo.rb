#!/usr/bin/env ruby

require 'optparse'

require_relative 'repubsub'
require 'rethinkdb'
include RethinkDB::Shortcuts

# Publishes messages to a simple string topic
def regex_publish
  exchange = Repubsub::Exchange.new(:regex_demo, :db => 'repubsub')

  loop do
    category, chartype, character = random_topic
    topic_key = "#{category}.#{chartype}.#{character}"
    payload = $CATEGORIES[category].sample

    puts "Publishing on topic #{topic_key}: #{payload}"

    exchange.topic(topic_key).publish(payload)
    sleep(0.5)
  end
end

# Subscribes to messages on a topic that match a regex
def regex_subscribe
  exchange = Repubsub::Exchange.new(:regex_demo, :db => 'repubsub')

  category, chartype, character = random_topic
  maybe_character = [character, '(.+)'].sample
  maybe_chartype = ["#{chartype}\.#{maybe_character}", '(.+)'].sample
  topic_regex = "^#{category}\.#{maybe_chartype}$"
  queue = exchange.queue{|topic| topic.match(topic_regex)}

  print_subscription = lambda do
    puts '=' * 20, "Subscribed to: #{topic_regex}", '=' * 20, ''
  end

  print_subscription.call

  queue.subscription.with_index do |(topic, payload), i|
    if i % 20 == 19
        # Reminder what we're subscribed to
        print_subscription.call
    end
    puts "Received on #{topic}: #{payload}"
  end
end


# Publishes messages with an array of tags as a topic
def tags_publish
  exchange = Repubsub::Exchange.new(:tags_demo, :db => 'repubsub')
  
  loop do
    # Get two random topics, remove duplicates, and sort them
    # Sorting ensures that if two topics consist of the same
    # tags, the same document in the database will be updated
    # This should result in 270 possible tag values
    topic_tags = (random_topic + random_topic).to_set.sort
    payload = ($TEAMUPS + $EVENTS + $FIGHTS).sample
    
    puts "Publishing on tags ##{topic_tags.join(' #')}", "\t #{payload}"

    exchange.topic(topic_tags).publish(payload)
    sleep(0.5)
  end
end

# Subscribes to messages that have specific tags in the topic
def tags_subscribe
  exchange = Repubsub::Exchange.new(:tags_demo, :db => 'repubsub')

  tags = random_topic.sample(2)
  queue = exchange.queue{|topic| topic.contains(*tags)}

  print_subscription = lambda do
    puts '='*20, "Subscribed to messages with tags ##{tags.join(' #')}", '='*20
  end
  
  print_subscription.call

  queue.subscription.with_index do |(topic, payload), i|
    if i % 10 == 9
      # Reminder what we're subscribed to
      print_subscription.call
    end
    puts "Received message with tags: ##{topic.join(' #')}", "\t #{payload}\n"
  end
end

# Publishes messages on a hierarchical topic
def hierarchy_publish
  exchange = Repubsub::Exchange.new(:hierarchy_demo, :db => 'repubsub')

  loop do
    topic_hash, payload = random_hierarchy
    
    puts "Publishing on a hierarchical topic:"
    print_hierarchy(topic_hash)
    puts " - #{payload}", ''

    exchange.topic(topic_hash).publish(payload)
    sleep(0.5)
  end
end

# Subscribes to messages on a hierarchical topic
def hierarchy_subscribe
  exchange = Repubsub::Exchange.new(:hierarchy_demo, :db => 'repubsub')
  
  category, chartype, character = random_topic
  queue = exchange.queue{|topic| topic[category][chartype].contains(character)}
  
  print_subscription = lambda do
    topic_query = "['#{category}']['#{chartype}'].contains('#{character}')"
    puts '='*20, "Subscribed to topic: #{topic_query}", '='*20, ''
  end

  print_subscription.call

  queue.subscription.with_index do |(topic,payload),i|
    if i % 5 == 4
      # Reminder what we're subscribed to
      print_subscription.call
    end

    puts 'Received message with topic:'
    print_hierarchy(topic)
    puts " - #{payload}", ''
  end
end

# Returns the pieces of a random topic
def random_topic
    category = $CATEGORIES.keys.sample
    chartype = $CHARACTERS.keys.sample
    character = $CHARACTERS[chartype].sample
    return category, chartype, character
end

# Returns a random hierarchical topic
def random_hierarchy
  # Default value is a Hash with a default value of an Array
  topic = Hash.new{|hash,key| hash[key] = Hash.new{|h,k| h[k] = []}}
  categories = []
  $CATEGORIES.keys.sample(1 + rand(2)).each do |category|
    categories.concat($CATEGORIES[category])
    $CHARACTERS.keys.sample(1 + rand(2)).each do |chartype|
      $CHARACTERS[chartype].sample(1 + rand(2)).each do |character|
        characters = topic[category][chartype] << character
        characters.sort!
      end
    end
  end
  return topic, categories.sample
end

# Prints a topic hierarchy nicely
def print_hierarchy(hash)
  hash.each_pair do |category,chartypes|
    puts "    #{category}:"
    chartypes.each_pair do |chartype,characters|
      puts "        #{chartype}: #{characters.join(', ')}"
    end
  end
end

# These are used in the demos
$CHARACTERS = {
    :superheroes => ['Batman', 'Superman', 'Captain America'],
    :supervillains => ['Joker', 'Lex Luthor', 'Red Skull'],
    :sidekicks => ['Robin', 'Jimmy Olsen', 'Bucky Barnes'],
}

$TEAMUPS = [
    "You'll never guess who's teaming up",
    'A completely one-sided fight between superheroes',
    'Sidekick goes on rampage. Hundreds given parking tickets',
    'Local politician warns of pairing between villains',
    'Unexpected coalition teams up to take on opponents',
]

$FIGHTS = [
    'A fight rages between combatants',
    'Tussle between mighty foes continues',
    'All out war in the streets between battling heroes',
    "City's greatest hero defeated!",
    "Villain locked in minimum security prison after defeat",
]

$EVENTS = [
    "Scientists accidentally thaw a T-Rex and release it",
    "Time vortex opens over downtown",
    "EMP turns out the lights. You'll never guess who turned them back on",
    "Inter-dimensional sludge released. Who can contain it?",
    "Super computer-virus disables all police cars. City helpless.",
]

$CATEGORIES = {
    :teamups => $TEAMUPS,
    :fights => $FIGHTS,
    :events => $EVENTS,
}

OptionParser.new do |opts|
  opts.banner = <<-BANNER
 usage: #{$PROGRAM_NAME} [-h] {regex,tags,hierarchy} {publish,subscribe}

 Demo for RethinkDB pub-sub

 positional arguments:
   {regex,tags,hierarchy}
                         Which demo to run
   {publish,subscribe}   Whether to publish or subscribe

 optional arguments:
   -h, --help            show this help message and exit
BANNER

  opts.on('-h') {|b| puts opts.banner; exit }
  opts.parse!
  demo = ARGV.shift
  demos = ['regex', 'tags', 'hierarchy']
  pub_or_sub = ARGV.shift
  pubsubs = ['publish', 'subscribe']
  if not demos.include?(demo)
    puts("invalid choice: '#{demo}' (choose from regex, tags, hierarchy)")
    puts opts.banner
    exit
  end
  if not pubsubs.include?(pub_or_sub)
    puts("invalid choice: '#{pub_or_sub}' (choose from publish, subscribe)")
    puts opts.banner
    exit
  end

  send("#{demo}_#{pub_or_sub}")
end
