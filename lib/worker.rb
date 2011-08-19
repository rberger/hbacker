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
module Stalker
  def log(msg); end
  def log_error(msg); end
end

module Worker
  require "hbacker"
  require 'logger'
  require "hbacker/cli"
  
  class WorkerError < RuntimeError ; end

  # Looks like Stalker allows for only a single error block for all jobs
  Stalker.error do |e, name, args, job, style_opts|
    puts "hello"
    Hbacker.log.error "WORKER ERROR: job: #{name} e: #{e.inspect}"
    puts "running"
    Hbacker.log.error e.backtrace.inspect
    puts "bye"
    
    if (e.class == RightAws::AwsError) && (e.include?(/ServiceUnavailable/))
      Hbacker.log.warn "ServiceUnavailable. Releasing job back on queue"
      job.release(job.pri, 10, job.ttr)
      break
    end

    if name =~ /import/
      @import_db_wrk.imported_table_info(args[:table_name], args[:session_name], false, {:info => "#{e.class}: #{e.message}"})
    elsif name =~ /export/
      @db_wrk.exported_table_info(args[:table_name], args[:start_time], args[:end_time], nil, \
        args[:versions], args[:session_name], false, {:info => "#{e.class}: #{e.message}"})
    end
    job.bury
  end

  ##
  # Stalker Job to do the work of starting a Hadoop Job to export an HBase Table
  # @param [Hash] args
  # @option [String] :table_name
  # @option [Integer] :start_time Earliest Time to export from (milliseconds since Unix Epoch)
  # @option [Integer] :end_time Latest Time to export to (milliseconds since Unix Epoch)
  # @option [String] :destination Full scheme://path for destination. Suitable for use with HBase/HDFS
  # @option [Integer] :versions Number of versions to export
  # @option [String] :session_name Name of the Export Session
  # @option [String] :stargate_url Full Schema/Path:Port URL to access the HBase stargate server
  # @option [String] :aws_access_key_id AWS key
  # @option [String] :aws_secret_access_key AWS secret
  # @option [String] :export_hbase_name Canonicalized HBase Cluster Name of the Export source
  # @option [String] :hbase_host HBase Master Hostname
  # @option [String] :hbase_port HBase Master Host Port
  # @option [String] :hbase_home Hadoop Home Directory
  # @option [String] :hadoop_home Hadoop Home Directory
  #
  Stalker.job 'queue_table_export' do |args, job, style_opts|
    a = Hbacker.transform_keys_to_symbols(args)
    Hbacker.log.level = a[:log_level] ? a[:log_level] : Logger::DEBUG

    # Hack to get around issues testing this module. Only called during testing
    @db_wrk = @hbase_wrk = @s3_wrk = @export_wrk = nil if a[:reset_instance_vars]

    @db_wrk ||= Hbacker::Db.new(:export,
                                Hbacker::CLI.get_db_conf,  #a[:db_config],
                                a[:export_hbase_name], a[:aws_access_key_id], a[:aws_secret_access_key], a[:reiteration_time])
    
    @hbase_wrk ||= Hbacker::Hbase.new(a[:hbase_home], a[:hadoop_home], a[:hbase_host], a[:hbase_port])
    @s3_wrk ||= Hbacker::S3.new(a[:aws_access_key_id], a[:aws_secret_access_key])
    @export_wrk ||= Hbacker::Export.new(@hbase_wrk, @db_wrk, a[:hbase_home], a[:hbase_version], a[:hadoop_home], @s3_wrk)

    has_rows = @hbase_wrk.table_has_rows?(a[:table_name])
      
    if has_rows
      if @hbase_wrk.wait_for_mapred_queue(a[:mapred_max_jobs], 10000, 2) != :ok
        raise WorkerError, "Export Timedout waiting #{10000 *2} seconds for Hadoop Map Reduce Queue to be less than #{a[:mapred_max_jobs]} jobs"
      end
      Hbacker.log.info "Exporting #{a[:table_name]} to #{a[:destination]}"
      @export_wrk.table(a[:table_name], a[:start_time], a[:end_time], a[:destination], a[:versions], a[:session_name])
    else
      table_descriptor = @hbase_wrk.table_descriptor(a[:table_name])
      Hbacker.log.warn "Worker#queue_table_export: Table: #{a[:table_name]} is empty. Recording in Db but not backing up"
      @db_wrk.exported_table_info(a[:table_name], a[:start_time], a[:end_time], table_descriptor,  a[:versions], a[:session_name], true)
    end
  end
  
  
  ##
  # Stalker Job to do the work of starting a Hadoop Job to import an HBase Table
  # @param [Hash] args
  # @option [String] :table_name
  # @option [String] source scheme://source_path/session_name/ to the previously exported data
  # @option [String] :session_name Name of the original Export Session
  # @option [String] :stargate_url Full Schema/Path:Port URL to access the HBase stargate server
  # @option [String] :aws_access_key_id AWS key
  # @option [String] :aws_secret_access_key AWS secret
  # @option [String] :export_hbase_name Canonicalized HBase Cluster Name of the Export Destination
  # @option [String] :import_hbase_name Canonicalized HBase Cluster Name of the Import Destination
  # @option [String] :hbase_host HBase Master Hostname
  # @option [String] :hbase_port HBase Master Host Port
  # @option [String] :hbase_home Hadoop Home Directory
  # @option [String] :hadoop_home Hadoop Home Directory
  #
  Stalker.job 'queue_table_import' do |args, job, style_opts|
    a = Hbacker.transform_keys_to_symbols(args)
    Hbacker.log.level = a[:log_level] ? a[:log_level] : Logger::DEBUG
    Hbacker.log.debug "#{a.inspect}"

    # Hack to get around issues testing this module. Only called during testing
    @export_db_wrk = @import_db_wrk = @hbase_wrk = @s3_wrk = @import_wrk = nil if a[:reset_instance_vars]
    
    @export_db_wrk ||= Hbacker::Db.new(:export, a[:db_config], 
      a[:export_hbase_name], a[:aws_access_key_id], 
      a[:aws_secret_access_key], a[:reiteration_time])
    @import_db_wrk ||= Hbacker::Db.new(:import, a[:db_config], 
      a[:import_hbase_name], a[:aws_access_key_id],
      a[:aws_secret_access_key], a[:reiteration_time])
      
    table_description = @import_db_wrk.column_descriptors(a[:table_name], a[:session_name])
    
    @hbase_wrk ||= Hbacker::Hbase.new(a[:hbase_home], a[:hadoop_home], a[:hbase_host], a[:hbase_port])
    @s3_wrk ||= Hbacker::S3.new(a[:aws_access_key_id], a[:aws_secret_access_key])
    @import_wrk ||= Hbacker::Import.new(@hbase_wrk, @export_db_wrk, @import_db_wrk, a[:hbase_home], 
      a[:hbase_version], a[:hadoop_home], @s3_wrk)

    if @hbase_wrk.wait_for_mapred_queue(a[:mapred_max_jobs], 10000, 2) != :ok
      raise WorkerError, "Import Timedout waiting #{10000 *2} seconds for Hadoop Map Reduce Queue to be less than #{a[:mapred_max_jobs]} jobs"
    end
    Hbacker.log.info "Importing  #{a[:table_name]} from #{a[:source]} import_session: #{ a[:import_session_name]}"
    @import_wrk.table(a[:table_name], a[:source], a[:import_session_name], table_description)
  end
  
end
