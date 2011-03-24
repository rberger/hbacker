require 'logger'
require "yaml"
require 'thor'
require 'hbacker/export'
require 'hbacker/import'
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
    
    # Common options
    class_option :tables, 
      :type => :array, 
      :aliases => "-t", 
      :desc => "Space separated list of tables"
    class_option :start, 
      :default => 0, 
      :aliases => "-s", 
      :desc => "Start time (millisecs since Unix Epoch)"
    class_option :end, 
      :default => now_minus_60_sec, 
      :aliases => "-s", 
      :desc => "End time (millisecs since Unix Epoch)"
    class_option :backup_timestamp, 
      :default => backup_timestamp,
      :desc => "Will be the top level folder in the destination directory specified by --dest_root"
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
    class_option :hbase_port, 
      :type => :string, 
      :default => 8080, 
      :aliases => "-P",
      :desc => "TCP Port of the hbase-stargate server"
    class_option :hbase_version, 
      :type => :string, 
      :default => "0.20.3", 
      :aliases => "-V",
      :desc => "Version of HBase of the source HBase"
    class_option :aws_config, 
      :type => :string, 
      :default => "~/.aws/aws_config.yml", 
      :aliases => "-c",
      :desc => "Yaml file with aws credentials and other config"
    class_option :hadoop_home, 
      :type => :string, 
      :default => "/mnt/hadoop", 
      :desc => "Local Unix file system path to where the Hadoop Home"
    class_option :hbase_home, 
      :type => :string, 
      :default => "/mnt/hbase",
      :desc => "Local Unix file system path to where the HBase Home"

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
      :default => 100000,
      :desc => "Number of versions of rows to back up per file"
    def export
      Hbacker.log.level = options[:debug] ? Logger::DEBUG : Logger::INFO
      
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
    method_option :backup_session, 
      :type => :string, 
      :desc => "Timestamp associated with the backup session. Will be appened to source_root      ", 
      :banner => "20110322_091701"
    method_option :pattern, 
      :type => :string, 
      :desc => "Regex for the table name within the Source scheme://path/backup_session/", 
      :banner => "\'*summary*\'"
    def import
      Hbacker.log.level = options[:debug] ? Logger::DEBUG : Logger::INFO
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

    no_tasks do
      ##
      # Initializes all the objects needed by the main tasks
      # Uses options and/or configuration files
      #
      def setup(task, options)
        config = YAML.load_file(File.expand_path(options[:aws_config]))
        hbase_name = options[:hbase_host].gsub(/[-\.]/, "_")
        db = Hbacker::Db.new(config['access_key_id'], config['secret_access_key'], hbase_name)
        hbase = Hbacker::Hbase.new(options[:hbase_home], options[:hadoop_home], options[:hbase_host], options[:hbase_port])
        
        case task
        when :export
          export = Export.new(hbase, db, options[:hbase_home], options[:hbase_version], options[:hadoop_home])
          config.merge({:hbase => hbase, :db => db, :hbase_name => hbase_name, :export => export})
        when :import
          s3 = Hbacker::S3.new(config['access_key_id'], config['secret_access_key'])
          import = Import.new(hbase, db, options[:hbase_home], options[:hbase_version], options[:hadoop_home], s3)
          config.merge({:hbase => hbase, :db => db, :hbase_name => hbase_name, :import => import})
        else
          Hbacker.log.error "Invalid task in CLI#setup: #{task}"
          exit(-1)
        end
      end
    end
  end
end