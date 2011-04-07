module Hbacker
  require "hbacker"
  
  class Import
    ##
    # Initialize the Import Instance
    #
    def initialize(hbase, db, hbase_home, hbase_version, hadoop_home, s3)
      @hbase = hbase
      @db = db
      @hadoop_home = hadoop_home
      @hbase_home = hbase_home
      @hbase_version = hbase_version
      @s3 = s3
    end

    ##
    # Master process to manage an Import session. Pulls data from :source_root Filesystem to specified HBase Cluster
    # @param [Hash] opts All then need options to run the export. Usually build by CLI. The following options are used
    # @option opts [String] :source_root Scheme://root_path of the Source directory of exports
    # @option opts [String] :session_name Name of the export session to import
    # 
    def specified_tables(opts)
      Hbacker.log.debug "Import#specified_tables"
      opts = Hbacker.transform_keys_to_symbols(opts)
      
      export_table_names = @db.table_names(:export, opts[:session_name], opts[:source_root])
      if opt[:tables]
      # Only export the tables specified in opts[:tables]
        export_table_names = export_table_names & opt[:tables]
        if export_table_names.lenght < opt[:tables].length
          raise Thor::InvocationError, "One or more of the tables requested does not exist in this backup"
        end
      end
      export_table_names.each do |table|
        source = "#{opts[:source_root]}#{opts[:session_name]}/#{table}/"
        Hbacker.log.info "Backing up #{table} to #{source}"
        self.table(table, source)
      end
    end
    
    # Queue a ruby job to manage the Hbase/Hadoop Import job
    def queue_table_import_job(table_name, start_time, end_time, source, 
      session_name, timeout, reiteration_time, mapred_max_jobs)
      args = {
        :table_name => table_name,
        :start_time => start_time,
        :end_time => end_time,
        :source => source,
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
      Hbacker.log.debug "------- Stalker.enqueue('queue_table_import_job', args, {:ttr => #{timeout}})"
      Stalker.enqueue('queue_table_import_job', args, {:ttr => timeout}, true, :no_bury_for_error_handler => true)
    end
    
    
    
    ##
    # Uses Hadoop to import specfied table from source file system to target HBase Cluster
    # @param [String] table_name The name of the table to import
    # @param [String] source scheme://source_path/session_name/ to the export data
    #
    def table(table_name, source)
      
      table_status = @hbase.create_table(table_name, table_description)
      
      cmd = "#{@hadoop_home}/bin/hadoop jar #{@hbase_home}/hbase-#{@hbase_version}.jar import " +
        "#{table_name} #{source}"
      Hbacker.log.debug "About to execute #{cmd}"
      cmd_output = `#{cmd} 2>&1`
      # Hbacker.log.debug "cmd output: #{cmd_output}"
      import_session_name = Hbacker::Cli.export_timestamp
      
      if $?.exitstatus > 0
        Hbacker.log.error"Hadoop command failed: #{cmd}"
        Hbacker.log.error cmd_output
        @s3.save_info("#{destination}hbacker_hadoop_import_error_#{import_session_name}.log", cmd_output)
        raise StandardError, "Error running Haddop Command", caller
      end
      @s3.save_info("#{destination}hbacker_hadoop_error.log", cmd_output)
    end
  end
end
