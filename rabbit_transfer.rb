#!/usr/bin/env ruby

require 'rubygems'
require 'bunny'
require 'json'
require 'yaml'
require 'optparse'
require 'fileutils'
require 'nori' # requires nokogiri
require 'logger'
require_relative 'lib/message_queue'

def process_options
  options = {}
  parser = OptionParser.new do |opts|
    opts.banner = 'Usage: amie_transfer.rb [options]'

    # option to override default config file location config/settings.yml
    opts.on('-c','--config file', 'Config File') do |config|
      options[:config] = config
    end

    # option to display help
    opts.on('-h','--help','Displays Help') do
      puts opts
      exit
    end
  end
  parser.parse!

  # exit if options file doesn't exist
  unless File.file? options[:config]
    puts 'No config file found'
    exit
  end
  options
end

def process_config(options)
  #Read in the configuration
  config = YAML.load_file(options[:config])
#  puts 'config ' + config.to_json
#  unless config.key?('restart_interval')
#    config['restart_interval'] = 3600
#  end
#  # TODO add error checking
  config
end

def xml_to_amie_data_hash(xml,logger)
  parser = Nori.new
  xml_data = parser.parse(xml)
  if xml_data['amie']
    return xml_data['amie']
  else
    logger.error("INVALID XML amie element not found")
    raise "amie element not found in xml"
  end
end

def get_type_and_version(amie_data_hash,config,logger)
  type = nil
  version = nil
  amie_data_hash.keys.each do |key|
    if key == '@version'
      version= amie_data_hash['@version']
      if version != '1.0'
        logger.error("WRONG VERSION #{version}")
        version = nil
      end
    else
      type = key
      if !config['valid_packet_types'].key?(type)
        logger.error("INVALID TYPE #{type}")
        type = nil
      end
    end
  end
  {type: type, version: version}
end

def validate_packet(amie_data,type,config)
  begin
    unless amie_data[type]['header']
      raise "Header element missing"
    end
    unless amie_data[type]['header']['originating_site_name']
      raise "Originating Site Name element missing"
    end
    unless config['amie']['local_site']
      raise "Configuration Local Site is missing"
    end
    unless config['amie']['remote_site']
      raise "Configuration Remote Site is missing"
    end
    unless amie_data[type]['header']['transaction_id']
      raise "Transaction ID is missing"
    end
    unless amie_data[type]['header']['packet_id']
      raise "Packet ID is missing"
    end
  end
end

def packet_type_priority(type,config)
  priority = 1
  if config['valid_packet_types'][type]
    priority = config['valid_packet_types'][type]
  end
  priority
end

puts "starting script"
options = process_options
config = process_config(options)
puts config.to_json
puts '/rabbitmq_transfer.log'
logger = Logger.new(config['amie']['log_folder'] + '/rabbitmq_transfer.log', 'monthly')
logger.level = Logger::INFO

#Connect to the MessageQueue
mq = MessageQueue.new(config['message_queue'])
mq_conn = mq.connection
ch = mq_conn.create_channel
ch.confirm_select

# Connect to exchange we will publish messages to
write_exchange = ch.topic(config['amie']['write_exchange'],durable: true, passive: true)

# Connect to the queue we will read messages from
read_queue = ch.queue(config['amie']['read_exchange'] + '.queue', durable: true, passive: true)

# read any messages sent to us
# this subscribe block keeps running in a background thread as long as the script is running
#   set manual ack so that we make sure we have the message safely stored before it is removed from the queue
read_queue.subscribe(:manual_ack => true) do |delivery_info, metadata, payload|
  message_valid = true

  # check the message version and type
  if message_valid
     amie_data = xml_to_amie_data_hash(payload,logger)
     type_and_version = get_type_and_version(amie_data,config,logger)
     type = type_and_version[:type]
     version = type_and_version[:version]
     if type.nil? or version.nil?
       message_valid = false
     end
  end

  # check that necessary fields are in the data
  begin
    validate_packet(amie_data, type, config)
  rescue StandardError => e
    logger.error("READING MESSAGE FAILED.  Required header fields missing #{e.to_s}")
    message_valid = false
  end

  if message_valid
    orig_site = amie_data[type]['header']['originating_site_name']
    local_site = config['amie']['local_site']
    remote_site = config['amie']['remote_site']
    transaction_id = amie_data[type]['header']['transaction_id']
    packet_id = amie_data[type]['header']['packet_id']
    time = Time.now.to_i.to_s
    filename = config['amie']['in_folder'] + '/' + "#{type}.#{orig_site}.#{transaction_id}.#{local_site}.#{remote_site}.#{packet_id}.#{time}.xml"

    begin
      File.open(filename, 'w') {|f| f.write(payload) }
      logger.info {"READ XML #{filename}"}
      logger.debug {"PAYLOAD #{payload}"}
      FileUtils.mv(filename,config['amie']['received_folder'])
      logger.info("MOVE FILE #{filename} to #{config['amie']['received_folder']}")
      # acknowledge the message
      ## this deletes it from the queue
      ## if the acknowledgement doesn't happen the message will be delivered again if this script stops and starts again
      ch.acknowledge(delivery_info.delivery_tag, false)
    rescue StandardError => e
       logger.info("COULD NOT WRITE FILE.  Leaving in message queue.  #{filename}")
    end
  else
    # message was invalid from the sender so we are done with it
    ch.acknowledge(delivery_info.delivery_tag, false)
    logger.info("INVALID MESSAGE: ACK #{filename}")
  end

end

# Send files
# Keep looping looking for new files in the outbox and send them when they are found
j=0
start_time = Time.now
puts 'CONFIG ' + config.to_json
while(true) do
  Dir[config['amie']['out_folder'] + '/*.xml'].each do |filename|
    message_valid = true
    content = IO.read(filename)
    logger.info("SENDING #{filename}")
    begin

      amie_data = xml_to_amie_data_hash(content,logger)
      type_and_version = get_type_and_version(amie_data,config,logger)
      type = type_and_version[:type]
      version = type_and_version[:version]

      # If the type or version are missing the xml is not valid
      if type.nil? or version.nil?
        message_valid = false
        logger.error("SEND ERROR type or version invalid")
        raise "Type or Version invalid"
      end
      if message_valid
          # check fields required to build the filename
          validate_packet(amie_data,type,config)
      end
    rescue StandardError => e
      # handle the exception when required fields are missing
      message_valid = false
      logger.error("DID NOT SEND.  INVALID XML #{e.to_s}")
      # move the file to the outgoing failed folder
      FileUtils.mv(filename,config['amie']['out_failed_folder'])
      logger.info("Moved #{filename} to #{config['amie']['out_failed_folder']}")
    end

    if message_valid
      basename = File.basename filename
      begin
       priority = packet_type_priority(type,config)
        write_exchange.publish(content,headers: {message_length: content.length}, :priority => priority, :routing_key => config['amie']['write_exchange'] + '.queue')
        success = ch.wait_for_confirms
        if success
          logger.info("SENT #{filename}")
          # if this is a response* packet move to done, otherwise move to wait
          if basename.match(/^response\./)
            FileUtils.mv(filename,config['amie']['done_folder'])
            logger.info("Moved #{filename} to #{config['amie']['done_folder']}")
          else
            FileUtils.mv(filename,config['amie']['wait_folder'])
            logger.info("Moved #{filename} to #{config['amie']['wait_folder']}")
          end
        else # not success, try restarting script
          logger.info ("ERROR Failed Publish #{filename}")
          mq_conn.close
          logger.close
          puts "restarting script"
          exec("#{__FILE__} -c #{options[:config]}" )
        end
      rescue StandardError => e
        logger.error("SEND ERROR #{e.to_s}")
      end
    end
  end
  sleep 4

end

mq_conn.close
exit
