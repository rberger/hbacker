module Hbacker
  module Worker
    require 'stalker'
    include Stalker
    
    ##
    # Stalker Job to do the work of starting a Hadoop Job to export an HBase Table
    # @option [String] table_name
    # @option [Integer] start_time Earliest Time to backup from (milliseconds since Unix Epoch)
    # @option [Integer] end_time Latest Time to backup to (milliseconds since Unix Epoch)
    # @option [String] destination Full scheme://path for destination. Suitable for use with HBase/HDFS
    # @option [Integer] versions Number of versions to backup
    # @option [String] backup_name Name of the Backup Session
    # @option [String] stargate_url Full Schema/Path:Port URL to access the HBase stargate server
    # @option [String] aws_access_key_id AWS key
    # @option [String] aws_secret_access_key AWS secret
    # @option [String] hbase_name Canonicalized HBase Cluster Name of the Backup source
    
    job 'queue_table_export' do |args|
      
    end
    
    def setup(args)
    end
  end
end
