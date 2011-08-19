# Copyright 2011 Robert J. Berger & Runa, Inc.
# 
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
# 
#      http://www.apache.org/licenses/LICENSE-2.0
# 
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.
#    
module Hbacker
  require "hbacker"
  require "stalker"
  require File.expand_path(File.join(File.dirname(__FILE__), "../", "stalker"))  
  
  class Import
    ##
    # Initialize the Import Instance
    #
    def initialize(hbase, export_db, import_db, hbase_home, hbase_version, hadoop_home, s3)
      @hbase = hbase
      @export_db = export_db
      @import_db = export_db
      @hadoop_home = hadoop_home
      @hbase_home = hbase_home
      @hbase_version = hbase_version
      @s3 = s3
    end

    class ImportError < HbackerError ; end
    class QueueTimeoutError < ImportError ; end
    class TableCreateError < ImportError ; end

    ##
    # Master process to manage an Import session. Pulls data from :source_root Filesystem to specified HBase Cluster
    # @param [Hash] opts All then need options to run the import. Usually build by CLI. The following options are used
    # @option opts [String] :source_root Scheme://root_path of the Source directory of previously exported data
    # @option opts [String] :session_name Name of the previously exported session to import
    # @option opts [String] :session_name Name of the previously exported session to import
    # 
    def specified_tables(opts)
      Hbacker.log.debug "Import#specified_tables"
      Hbacker.log.debug "#{opts.inspect}"
      begin
        opts = Hbacker.transform_keys_to_symbols(opts)

        @import_db.start_info(opts[:session_name], opts[:source_root], opts[:start_time], opts[:end_time], Time.now.utc)

        exported_table_names = @export_db.table_names(opts[:session_name], opts[:source_root])
        Hbacker.log.debug "import.rb/specified_tables/exported_table_names: #{exported_table_names.inspect}"
        if opts[:tables]
        # Only import the tables specified in opts[:tables]
          exported_table_names = exported_table_names & opts[:tables]
          if exported_table_names.length < opts[:tables].length
            Hbacker.log.debug "opts[:tables]: #{opts[:tables].inspect} exported_table_names: #{exported_table_names}"
            raise Thor::InvocationError, "One or more of the tables requested does not exist in this backup"
          end
        end
        exported_table_names.each do |table|
          source = "#{opts[:source_root]}#{opts[:session_name]}/#{table}/"
        
          wait_results = Hbacker.wait_for_hbacker_queue('queue_table_import', opts[:workers_watermark], opts[:workers_timeout])
          unless wait_results[:ok]
            msg = "Hbacker::Import#specified_tables: Timeout (#{opts[:workers_timeout]}) " +
              " waiting for workers in queue < opts[:workers_timeout]"
            Hbacker.log.error msg
            next
          end
      
          Hbacker.log.debug "Calling queue_table_import_job(#{table}, #{source}, " + 
            "#{opts[:session_name]}, #{opts[:import_session_name]}, #{opts[:timeout]}, " +
            "#{opts[:reiteration_time]}, #{opts[:mapred_max_jobs]}, #{opts[:restore_empty_tables]})"
          
          self.queue_table_import_job(table, source, opts[:session_name], 
            opts[:import_session_name], opts[:timeout], opts[:reiteration_time], 
            opts[:mapred_max_jobs], opts[:restore_empty_tables])
        end
      rescue Exception => e
        Hbacker.log.error "Hbacker::Import#specified_tables: EXCEPTION: #{e.inspect}"
        Hbacker.log.error caller.join("\n")
        @import_db.end_info(opts[:session_name], opts[:source_root], Time.now.utc, {:info => "#{e.class}: #{e.message}"})
        raise ImportError, "#{e.class}: #{e.message} #{e.backtrace}"
      end
      @import_db.end_info(opts[:session_name], opts[:source_root], Time.now.utc)
    end
    
    # Queue a ruby job to manage the Hbase/Hadoop Import job
    def queue_table_import_job(table_name, source, session_name, import_session_name, timeout,
                               reiteration_time, mapred_max_jobs, restore_empty_tables)
      args = {
        :table_name => table_name,
        :source => source,
        :session_name => session_name,
        :import_session_name => import_session_name,
        :stargate_url => @hbase.url,
        :aws_access_key_id => @export_db.aws_access_key_id,
        :aws_secret_access_key => @export_db.aws_secret_access_key,
        :export_hbase_name => @export_db.hbase_name,
        :import_hbase_name => @import_db.hbase_name,
        :hbase_host => @hbase.hbase_host,
        :hbase_port => @hbase.hbase_port,
        :hbase_home => @hbase_home,
        :hbase_version => @hbase_version,
        :hadoop_home => @hadoop_home,
        :s3 => @s3,
        :mapred_max_jobs => mapred_max_jobs,
        :log_level  => Hbacker.log.level,
        :reiteration_time => reiteration_time,
        :restore_empty_tables => restore_empty_tables
      }
      Hbacker.log.debug "------- Stalker.enqueue('queue_table_import', args, {:ttr => #{timeout}})"
      Stalker.enqueue('queue_table_import', args, {:ttr => timeout}, true, :no_bury_for_error_handler => true)
    end

    ##
    # Uses Hadoop to import specfied table from source file system to target HBase Cluster
    # @param [String] table_name The name of the table to import
    # @param [String] source scheme://source_path/session_name/ to the export data
    # @param [Boolean] Restore empty tables based on data stored in SimpleDB for the session (Not Implemented)
    #
    def table(table_name, source, import_session_name, table_description, restore_empty_tables=false)
      
      begin
        table_status = @hbase.create_table(table_name, table_description)
      rescue Hbase::TableFailCreateError
        Hbacker.log.warn "Hbacker::Import#table: Table #{name} already exists. Continuing"
      end
      
      raise TableCreateError, "Improper result from @hbase.create_table(#{table_name}, table_description)" unless table_status
      
      cmd = "#{@hadoop_home}/bin/hadoop jar #{@hbase_home}/hbase-#{@hbase_version}.jar import " +
        "#{table_name} #{source}"
      Hbacker.log.debug "About to execute #{cmd}"
      cmd_output = `#{cmd} 2>&1`
      # Hbacker.log.debug "cmd output: #{cmd_output}"
      
      if $?.exitstatus > 0
        Hbacker.log.error"Hadoop command failed: #{cmd}"
        Hbacker.log.error cmd_output
        @s3.save_info("#{source}hbacker_hadoop_import_error_import_#{import_session_name}.log", cmd_output)
        raise ImportError, "Error running Haddop Command", caller
      end
      @s3.save_info("#{source}hbacker_hadoop_import.log", cmd_output)
    end
  end
end
