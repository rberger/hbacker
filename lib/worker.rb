module Worker
  require "hbacker"
  ##
  # Stalker Job to do the work of starting a Hadoop Job to export an HBase Table
  # @param [Hash] args
  # @option [String] :table_name
  # @option [Integer] :start_time Earliest Time to backup from (milliseconds since Unix Epoch)
  # @option [Integer] :end_time Latest Time to backup to (milliseconds since Unix Epoch)
  # @option [String] :destination Full scheme://path for destination. Suitable for use with HBase/HDFS
  # @option [Integer] :versions Number of versions to backup
  # @option [String] :backup_name Name of the Backup Session
  # @option [String] :stargate_url Full Schema/Path:Port URL to access the HBase stargate server
  # @option [String] :aws_access_key_id AWS key
  # @option [String] :aws_secret_access_key AWS secret
  # @option [String] :hbase_name Canonicalized HBase Cluster Name of the Backup source
  # @option [String] :hbase_host HBase Master Hostname
  # @option [String] :hbase_port HBase Master Host Port
  # @option [String] :hbase_home Hadoop Home Directory
  # @option [String] :hadoop_home Hadoop Home Directory
  #
  Stalker.job 'queue_table_export' do |args|
    Stalker.log "Inside queue_table_export job"
    db = Hbacker::Db.new(args['access_key_id'], args['secret_access_key'], args['hbase_name'])
    hbase = Hbacker::Hbase.new(args[:hbase_home], args[:hadoop_home], args[:hbase_host], args[:hbase_port])
    export = Hbacker::Export.new(hbase, db, args[:hbase_home], args[:hbase_version], args[:hadoop_home])
    
    export.table(args[:table_name], args[:start_time], args[:end_time], args[:destination], 
      args[:versions], args[:backup_name])
  end
end
