require 'logger'
require 'thor'
require 'hbacker/export'
# require 'hbacker/import'
require 'hbacker/hbase'
require 'hbacker/db'

module Hbacker
  class CLI < Thor
    
    ##
    # Use (Now - 60 seconds) * 1000 to have a timestamp from 60 seconds ago in milliseconds
    #
    now_minus_60_sec = (Time.now.to_i - 60) * 1000

    desc "export", "Export HBase table[s]."
    method_options :incremental => false, :aliases => "-i", 
      :banner => "Do an incremental export. Use stored last backup. Ignores --start"
    method_options :all => false, :aliases => "-a", 
      :banner => "All tables in HBase"
    method_options :tables => [], :aliases => "-t", 
      :banner => "Space separated list of tables"
    method_options :destination => "s3n://runa-hbase-staging/", :aliases => "-d", :required => true,
      :banner  => "Destination S3 bucket, S3n path, HDFS or File"
    method_options :start => 0, :aliases => "-s", 
      :banner => "Start time (millisecs since Unix Epoch) Default: Shortly after Man first walked on Moon"
    method_options :end => now_minus_60_sec, :aliases => "-s", 
      :banner => "End time (millisecs since Unix Epoch) Default: Now - 60sec"
    method_options :debug => false, :aliases => "-d", 
      :banner => "Enable debug messages"
    method_options :hbase_host => "hbase_master0-staging.runa.com", :aliases => "-H",
      :banner => "Host name of the host running the hbase-stargate server"
    method_options :hbase_port => 8080, :aliases => "-P",
      :banner => "TCP Port of the hbase-stargate server"
    method_options :hbase_version => "0.20.3", :aliases => "-V",
      :banne => "Version of HBase of the source HBase"
    method_options :aws_config => "~/.aws/aws_config.yml", :aliases => "-c",
      :banner => "Yaml file with aws credentials and other config"
    method_options :hadoop_home => "/mnt/hadoop", 
      :banner => "Local Unix file system path to where the Hadoop Home (at least for Hadoop client config/jar)"
    method_options :hbase_home => "/mnt/hbase",
      :banner => "Local Unix file system path to where the HBase Home (at least for HBase client config/jar)"
    def export
      if options[:debug]
        HBacker.log.level = Logger::DEBUG
      else
        HBacker.log.level = Logger::WARN
      end
      
      Hbacker.log.debug "options: #{options.inspect}"
      if options[:all] && options[:tables]
        puts "Can only choose one of --all or --tables"
        help
        exit(-1)
      end
      
      config = setup(options)
      exp = config[:export]
      
      if options[:all]
        exp.all_tables options
      elsif options[:tables] && options[:destination]
        exp.specified_tables options
      else
        Hbacker.log.error "Invalid option combination"
        help
        exit(-1)
      end
    end

    def setup(options)
      config = YAML.load_file(File.join(ENV['HOME'], options[:aws_config]))
      hbase_name = options[:hbase_host].gsub(/[-\.]/, "_")
      Hbacker.log.debug "Hbacker::Db.new(#{config['access_key_id']}, #{config['secret_access_key']}, #{hbase_name})"
      db = Hbacker::Db.new(config['access_key_id'], config['secret_access_key'], hbase_name)
      
      Hbacker.log.debug "Hbacker::Hbase.new(#{options[:hbase_home]}, #{options[:hadoop_home]}, #{options[:hbase_host]}, #{options[:hbase_port]})"
      hbase = Hbacker::Hbase.new(options[:hbase_home], options[:hadoop_home], options[:hbase_host], options[:hbase_port])
      
      Hbacker.log.debug "export = Export.new(#{hbase}, #{db}, #{options[:hbase_home]}, #{options[:hbase_version]})"
      export = Export.new(hbase, db, options[:hbase_home], options[:hbase_version])
      config.merge({:hbase => hbase, :db => db, :hbase_name => hbase_name})
    end
  end
end