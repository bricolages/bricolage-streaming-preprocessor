#!/usr/bin/env ruby
# Usage: bundle exec ruby send-checkpoint.rb CONFIG_YML

require 'yaml'

config_path = ARGV[0] || 'config.yml'
config = YAML.load(File.read(config_path))
region = config['region']
ENV['AWS_REGION'] = region

require 'aws-sdk'
require 'json'
require 'uri'

queue_url = config['queue-url']
$stderr.puts "Queue: #{queue_url}"
args = { queue_url: queue_url, delay_seconds: 0 }
sqs = Aws::SQS::Client.new

send_message = -> (body) {
  args[:message_body] = {'Records' => [body]}.to_json
  sqs.send_message(**args)
}

body = {
  "eventSource" => "bricolage:system",
  "eventTime" => "2000-01-01T00:00:00.000Z",
  "eventName" => "checkpoint"
}
send_message.(body)
