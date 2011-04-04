module Hbacker
  require "hbacker"
  require "stalker"
  require File.expand_path(File.join(File.dirname(__FILE__), "../", "stalker"))  
  
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
      # begin
        Hbacker.log.debug "Export#specified_tables"
        opts = Hbacker.transform_keys_to_symbols(opts)

        @db.start_info(:export, opts[:session_name], opts[:dest_root], opts[:start], opts[:end], Time.now.utc)
        opts[:tables].each do |table_name|
        
          dest = "#{opts[:dest_root]}#{opts[:session_name]}/#{table_name}/"
        
          wait_results = Hbacker.wait_for_hbacker_queue('queue_table_export', opts[:workers_watermark], opts[:workers_timeout])
          unless wait_results[:ok]
            msg = "Hbacker::Export#specified_tables: Timeout (#{opts[:workers_timeout]}) " +
              " waiting for workers in queue < opts[:workers_timeout]"
            Hbacker.log.error msg
            raise Timeout::Error, msg
          end
        
          Hbacker.log.debug "Calling queue_table_export_job(#{table_name}, #{opts[:start]}, "+
            "#{opts[:end]}, #{dest}, #{opts[:versions]}, #{opts[:session_name]})"
          self.queue_table_export_job(table_name, opts[:start], opts[:end], dest, opts[:versions], 
            opts[:session_name], opts[:timeout], opts[:reiteration_time], opts[:mapred_max_jobs])
        end
      # rescue Exception => exception
      #   Hbacker.log.error "Hbacker::Export#specified_tables: EXCEPTION: #{exception}"
      #   Hbacker.log.error caller.join("\n")
      # end
    end
    
    ##
    # Queue a ruby job to manage the Hbase/Hadoop job
    def queue_table_export_job(table_name, start_time, end_time, destination, 
      versions, session_name, timeout, reiteration_time, mapred_max_jobs)
      args = {
        :table_name => table_name,
        :start_time => start_time,
        :end_time => end_time,
        :destination => destination,
        :versions => versions,
        :session_name => session_name,
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
        :mapred_max_jobs => mapred_max_jobs,
        :log_level  => Hbacker.log.level,
        :reiteration_time => reiteration_time
      }
      Hbacker.log.debug "------- Stalker.enqueue('queue_table_export', args, {:ttr => #{timeout}})"
      Stalker.enqueue('queue_table_export', args, {:ttr => timeout}, true, :no_bury_for_error_handler => true)
    end
    
    ##
    # * Uses Hadoop to export specfied table from HBase to destination file system
    # * Record that the date range and schema of table was exported to
    # @param [String] table_name
    # @param [Integer] start_time Earliest Time to export from (milliseconds since Unix Epoch)
    # @param [Integer] end_time Latest Time to export to (milliseconds since Unix Epoch)
    # @param [String] destination Full scheme://path for destination. Suitable for use with HBase/HDFS
    # @param [Integer] versions Number of versions to export
    # @param [String] session_name Name of the Export Session
    # @todo Check if table is empty, if so don't do hadoop job, just create the target directory and record in Db
    # @todo Make sure table is compacted before export
    #
    def table(table_name, start_time, end_time, destination, versions, session_name)
      table_descriptor = @hbase.table_descriptor(table_name)
      
      cmd = "#{@hadoop_home}/bin/hadoop jar #{@hbase_home}/hbase-#{@hbase_version}.jar export " +
        "#{table_name} #{destination} #{versions} #{start_time} #{end_time}"
      Hbacker.log.debug "------------------ About to execute #{cmd}"
      cmd_output = `#{cmd} 2>&1`
      # Hbacker.log.debug "cmd output: #{cmd_output}"
      
      if $?.exitstatus > 0
        Hbacker.log.error"Hadoop command failed:"
        Hbacker.log.error cmd_output
        @db.table_info(:export, table_name, start_time, end_time, table_descriptor, versions, session_name, false, true)
        Hbacker.log.debug "About to save_info to s3: #{destination}hbacker_hadoop_error.log"
        @s3.save_info("#{destination}hbacker_hadoop_error.log", cmd_output)
        raise StandardError, "Error running Haddop Command", caller
      end
      
      @db.table_info(:export, table_name, start_time, end_time, table_descriptor, versions, session_name, false, false)
      Hbacker.log.debug "About to save_info to s3: #{destination}hbacker_hadoop.log"
      @s3.save_info("#{destination}hbacker_hadoop.log", cmd_output)
    end
  end
end
