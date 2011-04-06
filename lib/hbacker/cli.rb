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
    class_option :hbase_host, 
      :type => :string, 
      :default => "hbase-master0-staging.runa.com", 
      :aliases => "-H",
      :desc => "Host name of the host running the hbase-stargate server"
    class_option :aws_config, 
      :type => :string, 
      :default => "~/.aws/aws_config.yml", 
      :aliases => "-c",
      :desc => "Yaml file with aws credentials and other config"

    desc "export", "Export HBase table[s]."
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
    method_option :start, 
      :type => :numeric,
      :default => 0, 
      :aliases => "-s", 
      :desc => "Start time (millisecs since Unix Epoch)"
    method_option :end, 
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
      :default => 1,
      :desc => "Number of jobs that need to be ready before more jobs are added to the queue"
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
      "--source_root       must be specified as it shows what type file system and bucket/path to table data. " +
      "If there are no --tables or --pattern specified, it will assume everything in " +
      "contained in --source_root       is a directory representing a table"
    method_option :source_root      , 
      :type => :string, 
      :default => "s3n://runa-hbase-staging/", 
      :aliases => "-S", 
      :required => true,
      :desc  => "Source scheme://path", 
      :banner => "s3 | s3n | hdfs | file"
    method_option :pattern, 
      :type => :string, 
      :desc => "SQL Wildcard (%) for the table name within the Source scheme://path/session_name/ Exp: %summary%"
    method_option :tables, 
      :type => :array, 
      :aliases => "-t", 
      :desc => "Space separated list of tables"
    method_option :start, 
      :default => 0, 
      :aliases => "-s", 
      :desc => "Start time (millisecs since Unix Epoch)"
    method_option :end, 
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
      :default => 1,
      :desc => "Number of jobs that need to be ready before more jobs are added to the queue"
    method_option :reiteration_time,
      :type => :numeric,
      :default => 15,
      :desc => "How many times the RightAws should try to complete an operation. Each time it backs off its delay by 2x"
    def import
      Hbacker.log.level = options[:debug] ? Logger::DEBUG : Logger::WARN
      config = setup(:import, options)
      imp = config[:export]
      
      if options[:all]
        imp.all_tables options
      elsif options[:tables] && options[:dest_root]
        imp.specified_tables options
      else
        Hbacker.log.error "Invalid option combination"
        help
        exit(-1)
      end
    end

    desc "db", "Query Export Meta Info DB"
    long_desc "Support functions to allow querying of the DB used to maintain information about exports" +
    "Options session_name and table_name allow the use of % for a wildcard at beginning and/or end"
    method_option :hbase_host, 
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
      
      config = setup(:db, options)
      db = config[:db]
      # Hbacker.log.debug "db: #{db.inspect} config:"
      # pp config if options[:debug]
      
      session_name = options[:session_name]
      table_name = options[:table_name]
      dest_root = options[:dest_root]
      
      exports = db.session_info(:export, session_name)
      
      exports.each do |export|
        session_name = export['session_name'].first
        attributes_string = ""
        export.each_pair do |k,v|
          next if %w(session_name id).include?(k)
          attributes_string += "#{k}: #{export[k]} "
        end
        puts "#{session_name}: #{attributes_string}"
        if table_name
          tables = db.list_table_info(:export, session_name, dest_root, table_name)
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
        hbase_name = options[:hbase_host].gsub(/[-\.]/, "_")
        db = Hbacker::Db.new(config['access_key_id'], config['secret_access_key'], hbase_name, options[:reiteration_time])
        hbase = Hbacker::Hbase.new(options[:hbase_home], options[:hadoop_home], options[:hbase_host], options[:hbase_port]) unless task == :db
        
        case task
        when :export
          s3 = Hbacker::S3.new(config['access_key_id'], config['secret_access_key'])
          export = Export.new(hbase, db, options[:hbase_home], options[:hbase_version], options[:hadoop_home], s3)
          config.merge({:hbase => hbase, :db => db, :hbase_name => hbase_name, :export => export})
        when :import
          s3 = Hbacker::S3.new(config['access_key_id'], config['secret_access_key'])
          import = Import.new(hbase, db, options[:hbase_home], options[:hbase_version], options[:hadoop_home], s3)
          config.merge({:hbase => hbase, :db => db, :hbase_name => hbase_name, :import => import})
        when :db
          config.merge({:hbase => hbase, :db => db, :hbase_name => hbase_name})
        else
          Hbacker.log.error "Invalid task in CLI#setup: #{task}"
          exit(-1)
        end
      end
    end
  end
end