module Stalker
  # def log(msg); end
  # def log_error(msg); end
end

module Worker
  require "hbacker"
  require 'logger'
  
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
  # @option [String] :hbase_name Canonicalized HBase Cluster Name of the Export source
  # @option [String] :hbase_host HBase Master Hostname
  # @option [String] :hbase_port HBase Master Host Port
  # @option [String] :hbase_home Hadoop Home Directory
  # @option [String] :hadoop_home Hadoop Home Directory
  #
  Stalker.job 'queue_table_export' do |args, job, style_opts|
    a = Hbacker.transform_keys_to_symbols(args)
    Hbacker.log.level = a[:log_level] ? a[:log_level] : Logger::DEBUG

    Stalker.error do |e, name, args, job, style_opts|
      stmt = "WORKER ERROR: job: #{name} e: #{e.inspect} args: #{args.inspect}"
      Hbacker.log.error stmt
      
      if e.include?(/ServiceUnavailable/)
        Hbacker.log.warn "ServiceUnavailable. Releasing job back on queue"
        job.release(job.pri, 10, job.ttr)
        break
      end
      stmt = "----------- After test for ServiceUnavailable"
      Hbacker.log.error stmt
    end

    # Hack to get around issues testing this module
    @db_wrk = @hbase_wrk = @s3_wrk = @export_wrk = nil if a[:reset_instance_vars]
    
    @db_wrk ||= Hbacker::Db.new(a[:aws_access_key_id], a[:aws_secret_access_key], a[:hbase_name], a[:reiteration_time])
    
    @hbase_wrk ||= Hbacker::Hbase.new(a[:hbase_home], a[:hadoop_home], a[:hbase_host], a[:hbase_port])
    @s3_wrk ||= Hbacker::S3.new(a[:aws_access_key_id], a[:aws_secret_access_key])
    @export_wrk ||= Hbacker::Export.new(@hbase_wrk, @db_wrk, a[:hbase_home], a[:hbase_version], a[:hadoop_home], @s3_wrk)

    has_rows = @hbase_wrk.table_has_rows?(a[:table_name])
      
    if has_rows
      if @hbase_wrk.wait_for_mapred_queue(a[:mapred_max_jobs], 10000, 2) != :ok
        raise Exception, "Timedout waiting #{10000 *2} seconds for Hadoop Map Reduce Queue to be less than #{a[:mapred_max_jobs]} jobs"
      end
      Hbacker.log.info "Backing up #{a[:table_name]} to #{a[:destination]}"
      @export_wrk.table(a[:table_name], a[:start_time], a[:end_time], a[:destination], a[:versions], a[:session_name])
    else
      table_descriptor = @hbase_wrk.table_descriptor(a[:table_name])
      Hbacker.log.warn "Worker#queue_table_export: Table: #{a[:table_name]} is empty. Recording in Db but not backing up"
      @db_wrk.table_info(:export, a[:table_name], a[:start_time], a[:end_time], table_descriptor,  a[:versions], a[:session_name], true, false)
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
  # @option [String] :hbase_name Canonicalized HBase Cluster Name of the Import Destination
  # @option [String] :hbase_host HBase Master Hostname
  # @option [String] :hbase_port HBase Master Host Port
  # @option [String] :hbase_home Hadoop Home Directory
  # @option [String] :hadoop_home Hadoop Home Directory
  #
  Stalker.job 'queue_table_import' do |args, job, style_opts|
    a = Hbacker.transform_keys_to_symbols(args)
    Hbacker.log.level = a[:log_level] ? a[:log_level] : Logger::DEBUG

    Stalker.error do |e, name, args, job, style_opts|
      stmt = "WORKER ERROR: job: #{name} e: #{e.inspect} args: #{args.inspect}"
      Hbacker.log.error stmt
      
      if e.include?(/ServiceUnavailable/)
        Hbacker.log.warn "ServiceUnavailable. Releasing job back on queue"
        job.release(job.pri, 10, job.ttr)
        break
      end
      stmt = "----------- After test for ServiceUnavailable"
      Hbacker.log.error stmt
    end

    # Hack to get around issues testing this module
    @db_wrk = @hbase_wrk = @s3_wrk = @import_wrk = nil if a[:reset_instance_vars]
    
    @db_wrk ||= Hbacker::Db.new(a[:aws_access_key_id], a[:aws_secret_access_key], a[:hbase_name], a[:reiteration_time])
    
    @hbase_wrk ||= Hbacker::Hbase.new(a[:hbase_home], a[:hadoop_home], a[:hbase_host], a[:hbase_port])
    @s3_wrk ||= Hbacker::S3.new(a[:aws_access_key_id], a[:aws_secret_access_key])
    @import_wrk ||= Hbacker::Import.new(@hbase_wrk, @db_wrk, a[:hbase_home], a[:hbase_version], a[:hadoop_home], @s3_wrk)

    table_status = @s3_wrk.list_table_info(:export, a[:table_name])
      
    if has_rows
      if @hbase_wrk.wait_for_mapred_queue(a[:mapred_max_jobs], 10000, 2) != :ok
        raise Exception, "Timedout waiting #{10000 *2} seconds for Hadoop Map Reduce Queue to be less than #{a[:mapred_max_jobs]} jobs"
      end
      Hbacker.log.info "Backing up #{a[:table_name]} to #{a[:source]}"
      @import_wrk.table(a[:table_name], a[:start_time], a[:end_time], a[:source], a[:versions], a[:session_name])
    else
      table_descriptor = @hbase_wrk.table_descriptor(a[:table_name])
      Hbacker.log.warn "Worker#queue_table_import: Table: #{table_name} is empty. Recording in Db but not backing up"
      @db_wrk.table_info(:import, a[:table_name], a[:start_time], a[:end_time], table_descriptor,  a[:versions], a[:session_name], true, false)
    end
  end
  
end
