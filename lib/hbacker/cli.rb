require 'logger'
require "yaml"
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
    backup_start = Time.now
    now_minus_60_sec = (backup_start.to_i - 60) * 1000
    backup_timestamp = backup_start.strftime("%Y%m%d_%H%M%S")

    desc "export", "Export HBase table[s]."
    method_option :incremental, :type => :boolean, :default => false, :aliases => "-i", 
      :desc => "Do an incremental export. Use stored last backup. Ignores --start"
    method_option :all, :type => :boolean, :default => false, :aliases => "-a", 
      :desc => "All tables in HBase"
    method_option :tables, :type => :array, :aliases => "-t", 
      :desc => "Space separated list of tables"
    method_option :destination, :type => :string, :default => "s3n://runa-hbase-staging/", :aliases => "-d", :required => true,
      :desc  => "Destination S3 bucket, S3n path, HDFS or File"
    method_option :backup_timestamp, :default => backup_timestamp,
      :desc => "Will be the top level folder in the destination directory specified by -destination"
    method_option :start, :default => 0, :aliases => "-s", 
      :desc => "Start time (millisecs since Unix Epoch)"
    method_option :end, :default => now_minus_60_sec, :aliases => "-s", 
      :desc => "End time (millisecs since Unix Epoch)"
    method_option :debug, :type => :boolean, :default => false, :aliases => "-d", 
      :desc => "Enable debug messages"
    method_option :hbase_host, :type => :string, :default => "hbase-master0-staging.runa.com", :aliases => "-H",
      :desc => "Host name of the host running the hbase-stargate server"
    method_option :hbase_port, :type => :string, :default => 8080, :aliases => "-P",
      :desc => "TCP Port of the hbase-stargate server"
    method_option :hbase_version, :type => :string, :default => "0.20.3", :aliases => "-V",
      :desc => "Version of HBase of the source HBase"
    method_option :aws_config, :type => :string, :default => "~/.aws/aws_config.yml", :aliases => "-c",
      :desc => "Yaml file with aws credentials and other config"
    method_option :hadoop_home, :type => :string, :default => "/mnt/hadoop", 
      :desc => "Local Unix file system path to where the Hadoop Home (at least for Hadoop client config/jar)"
    method_option :hbase_home, :type => :string, :default => "/mnt/hbase",
      :desc => "Local Unix file system path to where the HBase Home (at least for HBase client config/jar)"
    method_option :versions, :default => 100000,
      :desc => "Number of versions of rows to back up per file"
    def export
      if options[:debug]
        Hbacker.log.level = Logger::DEBUG
      else
        Hbacker.log.level = Logger::INFO
      end
      
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

    no_tasks do
      def setup(options)
        config = YAML.load_file(File.expand_path(options[:aws_config]))
        hbase_name = options[:hbase_host].gsub(/[-\.]/, "_")
        db = Hbacker::Db.new(config['access_key_id'], config['secret_access_key'], hbase_name)
        hbase = Hbacker::Hbase.new(options[:hbase_home], options[:hadoop_home], options[:hbase_host], options[:hbase_port])
        export = Export.new(hbase, db, options[:hbase_home], options[:hbase_version], options[:hadoop_home])
        config.merge({:hbase => hbase, :db => db, :hbase_name => hbase_name, :export => export})
      end
    end
  end
end