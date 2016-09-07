#!/usr/bin/env ruby
# Usage: bundle exec ruby send-s3-event.rb < SRC_S3_URL_LIST.txt

require 'yaml'

no_dispatch = ARGV.delete('--no-dispatch')
config_path = ARGV.shift || 'config.yml'
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

$stdin.each do |line|
  url = line.strip
  bucket, key = url.sub(%r<s3://>, '').split('/', 2)
  $stderr.puts "PUT bucket=#{bucket} key=#{key}"
  body = {
    "eventVersion" => "2.0",
    "eventSource" => "aws:s3",
    "awsRegion" => region,
    "eventTime" => "2000-01-01T00:00:00.000Z",
    "eventName" => "ObjectCreated:Put",
    "s3"=> {
      "s3SchemaVersion" => "1.0",
      "configurationId" => "LogStreamDev",
      "bucket"=> {
        "name" => bucket,
        "arn" => "arn:aws:s3:::#{bucket}"
      },
      "object"=> {
        "key"=> key,
        "size" => 1
      }
    }
  }
  body["noDispatch"] = true if no_dispatch
  send_message.(body)
end
