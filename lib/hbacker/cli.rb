require 'logger'
require "yaml"
require 'thor'
require 'hbacker/export'
require 'hbacker/import'
require 'hbacker/hbase'
require 'hbacker/db'

module Hbacker
  class CLI < Thor
    attr_reader :export_start
    ##
    # Use (Now - 60 seconds) * 1000 to have a timestamp from 60 seconds ago in milliseconds
    #
    @@export_start = Time.now.utc
    now_minus_60_sec = (@@export_start.to_i - 60) * 1000
    @@export_timestamp = @@export_start.strftime("%Y%m%d_%H%M%S")
    
    def self.export_start
      @@export_start
    end
    
    def self.export_timestamp
      @@export_timestamp
    end
    
    # Common options
    class_option :debug, 
      :type => :boolean, 
      :default => false, 
      :aliases => "-d", 
      :desc => "Enable debug messages"
    class_option :aws_config, 
      :type => :string, 
      :default => "~/.aws/aws_config.yml", 
      :aliases => "-c",
      :desc => "Yaml file with aws credentials and other config"

    desc "export", "Export HBase table[s]."
    method_option :export_hbase_host, 
      :type => :string, 
      :default => "hbase-master0-staging.runa.com", 
      :aliases => "-H",
      :desc => "Host name of the host running the hbase-stargate server and has the tables to be exported"
    method_option :all, 
      :type => :boolean, 
      :default => false, 
      :aliases => "-a", 
      :desc => "All tables in HBase"
    method_option :dest_root, 
      :type => :string, 
      :default => "s3n://runa-hbase-staging/", 
      :aliases => "-D", 
      :required => true,
      :desc  => "Destination root. S3 bucket, S3n path, HDFS or File"
    method_option :versions, 
      :type => :numeric,
      :default => 100000,
      :desc => "Number of versions of rows to back up per file"
    method_option :tables, 
      :type => :array, 
      :aliases => "-t", 
      :desc => "Space separated list of tables"
    method_option :start_time, 
      :type => :numeric,
      :default => 0, 
      :aliases => "-s", 
      :desc => "Start time (millisecs since Unix Epoch)"
    method_option :end_time, 
      :type => :numeric,
      :default => now_minus_60_sec, 
      :aliases => "-s", 
      :desc => "End time (millisecs since Unix Epoch)"
    method_option :session_name, 
      :default => @@export_timestamp,
      :type => :string,
      :desc => "String to select the export session. Exp: 20110327_224341",
      :banner => "STRING"
    method_option :hbase_port, 
      :type => :numeric,
      :default => 8080, 
      :aliases => "-P",
      :desc => "TCP Port of the hbase-stargate server"
    method_option :hbase_version, 
      :type => :string, 
      :default => "0.20.3", 
      :aliases => "-V",
      :desc => "Version of HBase of the source HBase"
    method_option :hadoop_home, 
      :type => :string, 
      :default => "/mnt/hadoop", 
      :desc => "Local Unix file system path to where the Hadoop Home"
    method_option :hbase_home, 
      :type => :string, 
      :default => "/mnt/hbase",
      :desc => "Local Unix file system path to where the HBase Home"
    method_option :mapred_max_jobs,
      :type => :numeric,
      :default => 6,
      :desc => "Will wait until the mapreduce job queue on hadoop cluster is less than this."
    method_option :timeout,
      :type => :numeric,
      :default => 30000,
      :desc  => "Stalker / Beanstalk job Timeout in seconds"
    method_option :workers_timeout,
      :type => :numeric,
      :default => 60000,
      :desc => "Timeout for waiting for # of workers in Beanstalk Queue to get less than workers_watermark"
    method_option :workers_watermark,
      :type => :numeric,
      :default => 0,
      :desc => "Export will wait until the number of ready jobs in the queue goes above this value before adding more Table Export jobs"
    method_option :reiteration_time,
      :type => :numeric,
      :default => 15,
      :desc => "How many times the RightAws should try to complete an operation. Each time it backs off its delay by 2x"
    def export
      Hbacker.log.level = options[:debug] ? Logger::DEBUG : Logger::WARN
      
      if options[:all] && options[:tables]
        Hbacker.log.error "Can only choose one of --all or --tables"
        help
        exit(-1)
      end
      
      config = setup(:export, options)
      exp = config[:export]
      
      if options[:all]
        exp.all_tables options
      elsif options[:tables] && options[:dest_root]
        exp.specified_tables options
      else
        Hbacker.log.error "Invalid option combination"
        help
        exit(-1)
      end
    end

    desc "import", "Import HBase table[s]."
    long_desc "Import HBase tables from a specified source. " +
      "If there are no --tables or --pattern specified, it will assume everything " +
      "contained in --source_root/-session_name is a directory representing tables to be imported"
    method_option :export_hbase_host, 
      :type => :string, 
      :default => "hbase-master0-staging.runa.com", 
      :aliases => "-H",
      :desc => "Host name of the hbase master / stargate server of the original Hbase cluster tables were exported from"
    method_option :import_hbase_host, 
      :type => :string, 
      :required => true, 
      :aliases => "-I",
      :desc => "Hbase master / stargate host of the Hbase cluster that is the destinaton of the Import. Example: hbase-master0-staging.runa.com"
    method_option :source_root, 
      :type => :string, 
      :required => true, 
      :aliases => "-S", 
      :required => true,
      :desc  => "Source scheme://path. Example: s3n://runa-hbase-staging/", 
      :banner => "s3 | s3n | hdfs | file"
    method_option :pattern, 
      :type => :string, 
      :desc => "SQL Wildcard (%) for the table name within the Source scheme://path/session_name/ Exp: %summary%"
    method_option :tables, 
      :type => :array, 
      :aliases => "-t", 
      :desc => "Optional list of table names to import. Will import all tables that were exported for the specified session_name"
    method_option :start_time, 
      :default => 0, 
      :aliases => "-s", 
      :desc => "Start time (millisecs since Unix Epoch)"
    method_option :end_time, 
      :type => :numeric,
      :default => now_minus_60_sec, 
      :aliases => "-s", 
      :desc => "End time (millisecs since Unix Epoch)"
    method_option :session_name, 
      :required => true,
      :type => :string,
      :desc => "String to select the export session. Exp: 20110327_224341"
    method_option :hbase_port, 
      :type => :numeric,
      :default => 8080, 
      :aliases => "-P",
      :desc => "TCP Port of the hbase-stargate server"
    method_option :hbase_version, 
      :type => :string, 
      :default => "0.20.3", 
      :aliases => "-V",
      :desc => "Version of HBase of the source HBase"
    method_option :hadoop_home, 
      :type => :string, 
      :default => "/mnt/hadoop", 
      :desc => "Local Unix file system path to where the Hadoop Home"
    method_option :hbase_home, 
      :type => :string, 
      :default => "/mnt/hbase",
      :desc => "Local Unix file system path to where the HBase Home"
    method_option :mapred_max_jobs,
      :type => :numeric,
      :default => 6,
      :desc => "Will wait until the mapreduce job queue on hadoop cluster is less than this."
    method_option :timeout,
      :type => :numeric,
      :default => 30000,
      :desc  => "Stalker / Beanstalk job Timeout in seconds"
    method_option :workers_timeout,
      :type => :numeric,
      :default => 60000,
      :desc => "Timeout for waiting for # of workers in Beanstalk Queue to get less than workers_watermark"
    method_option :workers_watermark,
      :type => :numeric,
      :default => 0,
      :desc => "Export will wait until the number of ready jobs in the queue goes above this value before adding more Table Import jobs"
    method_option :reiteration_time,
      :type => :numeric,
      :default => 15,
      :desc => "How many times the RightAws should try to complete an operation. Each time it backs off its delay by 2x"
    method_option :restore_empty_tables,
      :type => :boolean,
      :desc => "Not yet implemented"
      # :desc => "Enable the recreation of empty tables if the original source had empty tables"
    def import
      Hbacker.log.level = options[:debug] ? Logger::DEBUG : Logger::WARN
      raise Thor::MalformattedArgumentError, "Can not set bot --tables and --pattern" if options[:tables] && options[:pattern]
 
      config = setup(:import, options)
      imp = config[:export]
      imp.specified_tables options
    end

    desc "db", "Query Export Meta Info DB"
    long_desc "Support functions to allow querying of the DB used to maintain information about exports" +
    "Options session_name and table_name allow the use of % for a wildcard at beginning and/or end"
    method_option :export_hbase_host, 
      :type => :string, 
      :default => "hbase-master0-staging.runa.com", 
      :aliases => "-H",
      :desc => "Name of Hbase master server to find exports for",
      :required => true
    method_option :table_name,
      :type => :string,
      :desc => "Optional. Used to limit which tables will be displayed " +
        "Exp: %staging_consumer_events%",
      :banner => "STRING"
    method_option :session_name, 
      :type => :string,
      :desc => "String to select the export session. Exp: 20110327_%",
      :banner => "STRING"
    method_option :dest_root,
      :type => :string,
      :desc => "Limit exports to ones that saved in this locaiton",
      :default => "s3n://runa-hbase-staging/",
      :required => true
    def db
      Hbacker.log.level = options[:debug] ? Logger::DEBUG : Logger::WARN
      
      config = setup(:export_db, options)
      export_db = config[:export_db]
      # Hbacker.log.debug "export_db: #{export_db.inspect} config:"
      # pp config if options[:debug]
      
      session_name = options[:session_name]
      table_name = options[:table_name]
      dest_root = options[:dest_root]
      
      exports = export_db.session_info(:export, session_name)
      
      exports.each do |export|
        session_name = export['session_name'].first
        attributes_string = ""
        export.each_pair do |k,v|
          next if %w(session_name id).include?(k)
          attributes_string += "#{k}: #{export[k]} "
        end
        puts "#{session_name}: #{attributes_string}"
        if table_name
          tables = export_db.list_table_info(:export, session_name, dest_root, table_name)
          tables.each do |table|
            attributes_string = ""
            table.each_pair do |k,v|
              next if k == :table_name
              next if %w(table_name id).include?(k)
              attributes_string += "#{k}: #{v} "
            end
            puts "#{table[:table_name]}: #{attributes_string}"
          end
        end
      end
    end

    no_tasks do
      ##
      # Initializes all the objects needed by the main tasks
      # Uses options and/or configuration files
      #
      def setup(task, options)
        config = YAML.load_file(File.expand_path(options[:aws_config]))
        
        if [:export, :export_db, :import].inclue?(task)
          export_hbase_name = options[:export_hbase_host].gsub(/[-\.]/, "_")
          export_db = Hbacker::Db.new(config['access_key_id'], config['secret_access_key'], export_hbase_name, options[:reiteration_time])
        end
        
        if [:import, :import_db].inclue?(task)
          import_hbase_name = options[:import_hbase_host].gsub(/[-\.]/, "_")
          import_db = Hbacker::Db.new(config['access_key_id'], config['secret_access_key'], import_hbase_name, options[:reiteration_time])
        end
        
        unless [:export_db, :export_db].include?(task)
          s3 = Hbacker::S3.new(config['access_key_id'], config['secret_access_key'])
          hbase = Hbacker::Hbase.new(options[:hbase_home], options[:hadoop_home], options[:export_hbase_host], options[:hbase_port])
        end
        
        case task
        when :export
          export = Export.new(hbase, export_db, options[:hbase_home], options[:hbase_version], options[:hadoop_home], s3)
          config.merge({:hbase => hbase, :export_db => export_db, :export_hbase_name => export_hbase_name, :export => export})
        when :import
          import_hbase_name = options[:import_hbase_host].gsub(/[-\.]/, "_")
          import = Import.new(hbase, export_db, import_db, options[:hbase_home], options[:hbase_version], options[:hadoop_home], s3)
          config.merge({:hbase => hbase, :export_db => export_db, :import_db => import_db, :import_hbase_name => import_hbase_name, :import => import})
        when :export_db
          config.merge({:export_db => export_db, :export_hbase_name => export_hbase_name})
        when :import_db
          config.merge({:import_db => import_db, :import_hbase_name => import_hbase_name})
        else
          Hbacker.log.error "Invalid task in CLI#setup: #{task}"
          exit(-1)
        end
      end
    end
  end
end