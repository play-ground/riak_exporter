require 'riak'
require 'json'
require 'optparse'

class RiakDump
  def initialize(options)
    Riak.disable_list_keys_warnings = true
    @options = options

    @client = Riak::Client.new(:nodes => [
      { :host => options[:host],
        :protocol => "pbc"}
    ])
  end

  def dump
    counter = 0
    output = File.open(@options[:file], 'w:UTF-8')
    buckets = @options[:bucket] ? [ @client.bucket(@options[:bucket]) ] : @client.buckets
    buckets.each do | bucket|
      puts "Dumping bucket: #{bucket.name}"
      bucket.keys.each do |key|
        object = bucket.get(key)
        begin
          output.write("#{bucket.name}:#{key}:")
          output.write(object.raw_data)
        rescue => ex
          $stderr.puts "Error with: #{bucket.name}/#{key} - #{ex.message[0..75]}"
        ensure
          output.write("\n")
        end
        print '.' if @options[:verbose]
        counter += 1
        return if @options[:count] && @options[:count] <= counter
      end
    end
    puts "Dumped #{counter} records."
  ensure
    output.close
  end

  def load
    counter = 0
    File.open(@options[:file]) do |file|
      while (line = file.gets)
        counter += 1
        break if @options[:count] && @options[:count] < counter
        next if line.chomp!.empty?

        bucket_name, _, line = line.partition(':')
        key, _, line = line.partition(':')
        begin
          hash = JSON.parse(line)
          bucket_name = @options[:bucket] if @options[:bucket]
          puts "Inserting: #{bucket_name}/#{key} - #{line[0..40]}..." if @options[:verbose]
          obj = @client.bucket(bucket_name).new(key)
          obj.data = hash
          obj.store
        rescue => ex
          $stderr.puts "Failed #{counter}: #{bucket_name}/#{key} from #{line[0..50]}- #{ex.message[0..50]}"
        end
      end
    end
    puts "Finished processing #{counter} records in file!"
  end

  def read_ticket(bucket_id, key)
    bucket = @client.bucket(bucket_id)
    obj = bucket.get(key)
    puts "#{key}: #{obj.inspect}"
  end
end

options = {}
OptionParser.new do |opts|
  opts.banner = 'Usage: riak_dump.rb [options]'

  opts.on('-v', '--verbose', 'Run verbosely') do
    options[:verbose] = true
  end

  opts.on('-d', '--dump', 'Dump data from Riak') do
    options[:action] = :dump
  end

  opts.on('-l', '--load', 'Load data into Riak') do
    options[:action] = :load
  end

  opts.on('-f', '--file FILE', 'File to load or dump the data from/to.') do |file|
    options[:file] = file
  end

  opts.on('-b', '--bucket BUCKET', '(Optional) Bucket name to load or dump.') do |bucket|
    options[:bucket] = bucket
  end

  opts.on('-c', '--count COUNT', '(Optional) Limit records loaded/dumped to this number.') do |count|
    options[:count] = count.to_i
  end

  opts.on('-h', '--host HOST',
          'Hostname or IP to connect to, will use default port.') do |host|
    options[:host] = host
  end
end.parse!

if !options[:action]
  puts "Please specify either '-d' or '-l' flag"
elsif !options[:file]
  puts 'Specify the file to load or dump.'
elsif !options[:host]
  puts 'Specify the Riak hostname or IP to connect to.'
else
  rd = RiakDump.new(options)
  if options[:action] == :dump
    rd.dump
  else
    rd.load
  end
end

# rd.read_ticket("tickets_staging_aws_pod100", "tickets_20007561")