module Hbacker
  require "hbacker"
  require "stalker"
  
  class Export
    # attr_reader :hadoop_home, :hbase_home
    ##
    # Initialize the Export Instance
    #
    def initialize(hbase, db, hbase_home, hbase_version, hadoop_home, s3)
      @hbase = hbase
      @db = db
      @s3 = s3
      @hadoop_home = hadoop_home
      @hbase_home = hbase_home
      @hbase_version = hbase_version
    end

    ##
    # Querys HBase to get a list of all the tables in the cluser
    # Iterates thru the list calling Export#table to do the Export to the specified dest
    # @param [Hash] opts Hash from the CLI with all the options set
    # 
    def all_tables(opts)
      Hbacker.log.debug "Export#all_tables"
      new_opts = {}
      # Had to do this since options from Thor seem to be frozen
      # Also making sure keys are symbols
      opts.each_pair { |k,v| new_opts[k.to_sym] =v}
      
      new_opts[:tables] = @hbase.list_names_of_all_tables
      specified_tables(new_opts)
    end
    
    ##
    # Iterates thru the list of tables calling Export#table to do the Export to the specified dest
    # @param [Hash] opts Hash from the CLI with all the options set
    #
    def specified_tables(opts)
      Hbacker.log.debug "Export#specified_tables"
      opts = Hash.transform_keys_to_symbols(opts)

      @db.record_backup_start(opts[:backup_name], opts[:dest_root], opts[:start], opts[:end], Time.now.utc)
      opts[:tables].each do |table_name|
        
        dest = "#{opts[:dest_root]}#{opts[:backup_name]}/#{table_name}/"
        Hbacker.log.debug "Calling queue_table_export_job(#{table_name}, #{opts[:start]}, "+
          "#{opts[:end]}, #{dest}, #{opts[:versions]}, #{opts[:backup_name]})"
        self.queue_table_export_job(table_name, opts[:start], opts[:end], dest, opts[:versions], 
          opts[:backup_name], opts[:timeout])
      end
    end
    
    ##
    # Queue a ruby job to manage the Hbase/Hadoop job
    def queue_table_export_job(table_name, start_time, end_time, destination, versions, backup_name, timeout)
      args = {
        :table_name => table_name,
        :start_time => start_time,
        :end_time => end_time,
        :destination => destination,
        :versions => versions,
        :backup_name => backup_name,
        :stargate_url => @hbase.url,
        :aws_access_key_id => @db.aws_access_key_id,
        :aws_secret_access_key => @db.aws_secret_access_key,
        :hbase_name => @db.hbase_name,
        :hbase_host => @hbase.hbase_host,
        :hbase_port => @hbase.hbase_port,
        :hbase_home => @hbase_home,
        :hbase_version => @hbase_version,
        :hadoop_home => @hadoop_home,
        :s3 => @s3,
        :log_level  => Hbacker.log.level
      }
      Hbacker.log.debug "------- ENQUEUING #{args.inspect}"
      Stalker.enqueue('queue_table_export', args, {:ttr => timeout})
    end
    
    ##
    # * Uses Hadoop to export specfied table from HBase to destination file system
    # * Record that the date range and schema of table was exported to
    # @param [String] table_name
    # @param [Integer] start_time Earliest Time to backup from (milliseconds since Unix Epoch)
    # @param [Integer] end_time Latest Time to backup to (milliseconds since Unix Epoch)
    # @param [String] destination Full scheme://path for destination. Suitable for use with HBase/HDFS
    # @param [Integer] versions Number of versions to backup
    # @param [String] backup_name Name of the Backup Session
    # @todo Check if table is empty, if so don't do hadoop job, just create the target directory and record in Db
    # @todo Make sure table is compacted before backup
    #
    def table(table_name, start_time, end_time, destination, versions, backup_name)
      table_descriptor = @hbase.table_descriptor(table_name)
      
      cmd = "#{@hadoop_home}/bin/hadoop jar #{@hbase_home}/hbase-#{@hbase_version}.jar export " +
        "#{table_name} #{destination} #{versions} #{start_time} #{end_time}"
      Hbacker.log.debug "------------------ About to execute #{cmd}"
      cmd_output = `#{cmd} 2>&1`
      # Hbacker.log.debug "cmd output: #{cmd_output}"
      
      if $?.exitstatus > 0
        Hbacker.log.error"Hadoop command failed:"
        Hbacker.log.error cmd_output
        @db.record_table_info(table_name, start_time, end_time, table_descriptor, versions, backup_name, false, true)
        Hbacker.log.debug "About to save_info to s3: #{destination}hbacker_hadoop_error.log"
        @s3.save_info("#{destination}hbacker_hadoop_error.log", cmd_output)
        raise StandardError, "Error running Haddop Command", caller
      end
      
      @db.record_table_info(table_name, start_time, end_time, table_descriptor, versions, backup_name, false, false)
      Hbacker.log.debug "About to save_info to s3: #{destination}hbacker_hadoop.log"
      @s3.save_info("#{destination}hbacker_hadoop.log", cmd_output)
    end
  end
end
