#!/usr/bin/env ruby

require 'yaml'

config = YAML.load(File.read("config.yml"))
region = config['region']
ENV['AWS_REGION'] = region

require 'aws-sdk'
require 'json'
require 'pp'
require 'pry'

queue_url = config['queue-url']
$stderr.puts queue_url

sqs = Aws::SQS::Client.new

deleted = true
while deleted
  deleted = false

  result = sqs.receive_message(
    queue_url: queue_url,
    max_number_of_messages: 10,
    visibility_timeout: 300,
    wait_time_seconds: 10
  )
  unless result.successful?
    $stderr.puts "receive_message failed: #{result.error.inspect}"
    exit 1
  end
  result.messages.each do |msg|
    body = JSON.parse(msg.body)
    $stderr.puts body['Records'].inspect
    res = sqs.delete_message(queue_url: queue_url, receipt_handle: msg.receipt_handle)
    $stderr.puts "deleted: #{res.successful?}"
    deleted = true if res.successful?
  end
end
