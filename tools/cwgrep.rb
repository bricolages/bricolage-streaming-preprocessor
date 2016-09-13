#!/usr/bin/env ruby

require 'aws-sdk'
require 'optparse'
require 'pp'

log_group = 'ecs'
log_stream_prefix = nil
latest_hours = 1
start_time = (Time.now.to_i - (latest_hours * 60 * 60)) * 1000
pattern = nil
list_stream = false

opts = OptionParser.new("Usage: #{$0} --stream-prefix=PREFIX SEARCH_PATTERN")
opts.on('-g', '--log-group=NAME', "Log group name. (default: #{log_group})") {|name|
  log_group = name
}
opts.on('-p', '--stream-prefix=NAME', '[required] Log stream prefix.') {|prefix|
  log_stream_prefix = prefix
}
opts.on('-h', '--latest-hours=N', "Searches latest N hours. (default: #{latest_hours} hours)") {|n|
  if n.to_i < 1
    $stderr.puts "invalid hour (--latest-hours): #{n.inspect}"
    exit 1
  end
  start_time = (Time.now.to_i - (n.to_i * 60 * 60)) * 1000
}
opts.on('-l', '--list-streams', 'Lists stream names without search.') {
  list_stream = true
}
opts.on('--help', 'Prints this message and quit.') {
  puts opts.help
  exit 0
}

def error_exit(msg)
  $stderr.puts msg
  exit 1
end

begin
  opts.parse!
rescue OptionParser::ParseError => ex
  error_exit ex.message
end
error_exit "missing -p/--stream-prefix" unless log_stream_prefix
unless list_stream
  error_exit "missing search pattern" if ARGV.empty?
  error_exit "wrong number of arguments (#{ARGV.size} for 1)" if ARGV.size > 1
  pattern = ARGV[0]
end

cw = Aws::CloudWatchLogs::Client.new

res = cw.describe_log_streams(log_group_name: log_group, log_stream_name_prefix: log_stream_prefix)
stream_names = res.log_streams.map {|s| s.log_stream_name }
if list_stream
  puts stream_names
  exit 0
end

next_token = nil
begin
  res = cw.filter_log_events(
    log_group_name: log_group,
    log_stream_names: stream_names,
    start_time: start_time,
    filter_pattern: %Q({$.log = "*#{pattern}*"}),
    next_token: next_token
  )
  res.events.each do |e|
    puts "#{e.timestamp}: #{e.log_stream_name}: #{e.message}"
  end
  next_token = res.next_token
end while res.next_token
