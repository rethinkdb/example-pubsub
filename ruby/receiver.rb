#!/usr/bin/env ruby

require 'logger'
require_relative 'rethinkmq'

$USAGE = <<USAGE
Usage: #{$PROGRAM_NAME} <topic_pattern> [<topic_pattern>...]

Examples:

  Get all messages for temperature topics in the US or UK
    #{$PROGRAM_NAME} weather.us.temp weather.uk.temp

  Get all messages for any state in the US named Springfield
    #{$PROGRAM_NAME} 'weather.us.*.springfield'

  Get all temperature messages no matter where they're from
    #{$PROGRAM_NAME} 'weather.#.temp'
USAGE

def main
  if ARGV[0] == '-h'
    puts $USAGE
    return
  elsif ARGV[0] == '-v'
    ARGV.shift
  else
    $RMQLogger.level = Logger::ERROR
  end

  queue = MQConnection.new('MQ').exchange('messages').queue(*ARGV)

  queue.consume do |topic, payload|
    puts "[#{topic}] received:"
    puts "    #{payload}"
  end
end

main
