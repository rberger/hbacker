module Stalker
  def log(msg); end
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
    args = Hbacker.transform_keys_to_symbols(args)
    Hbacker.log.level = args[:log_level] ? args[:log_level] : Logger::DEBUG

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


    
    # Turn args hash into instance variables. These are read only
    args.each_pair do |k,v|
      self.instance_variable_set("@#{k}", v)
      self.class.send(:define_method, k, proc{self.instance_variable_get("@#{k}")})
    end

    puts "Worker First @hbase_mock: #{@hbase_mock.inspect} @hbase_wrk: #{@hbase_wrk.inspect}"
    
    @db_wrk ||= Hbacker::Db.new(aws_access_key_id, aws_secret_access_key, hbase_name, reiteration_time)
    
    @hbase_wrk ||= Hbacker::Hbase.new(hbase_home, hadoop_home, hbase_host, hbase_port)
    @s3_wrk ||= Hbacker::S3.new(aws_access_key_id, aws_secret_access_key)
    @export_wrk ||= Hbacker::Export.new(@hbase_wrk, @db_wrk, hbase_home, hbase_version, hadoop_home, @s3_wrk)
    puts "Worker @hbase_wrk: #{@hbase_wrk.inspect}"
    has_rows = @hbase_wrk.table_has_rows?(table_name)
      
    if has_rows
      if @hbase_wrk.wait_for_mapred_queue(mapred_max_jobs, 10000, 2) != :ok
        raise Exception, "Timedout waiting #{10000 *2} seconds for Hadoop Map Reduce Queue to be less than #{mapred_max_jobs} jobs"
      end
      Hbacker.log.info "Backing up #{table_name} to #{destination}"
      @export_wrk.table(table_name, start_time, end_time, destination, versions, session_name)
    else
      table_descriptor = @hbase_wrk.table_descriptor(table_name)
      Hbacker.log.warn "Worker#queue_table_export: Table: #{table_name} is empty. Recording in Db but not backing up"
      @db_wrk.table_info(:export, table_name, start_time, end_time, table_descriptor,  versions, session_name, true, false)
    end

  end
end
