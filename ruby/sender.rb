#!/usr/bin/env ruby
# This is a simple message sender application that uses the rethinkmq
# library.

# It receives a message at the command line and sends it to the
# specified topic

require 'logger'
require_relative 'rethinkmq'

$USAGE = <<USAGE
Usage: #{$PROGRAM_NAME} [-v] <topic> <message>

Examples:

  Broadcast the temperature in Mountain View
    #{$PROGRAM_NAME} weather.us.ca.mountainview 74
  Broadcast the temperature in Mountain View
    #{$PROGRAM_NAME} weather.uk.conditions "A bit rainy"

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

  MQConnection.new('MQ').exchange('messages').topic(ARGV[0]).publish(ARGV[1])
end

main
