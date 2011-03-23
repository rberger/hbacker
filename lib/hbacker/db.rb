module Hbacker
  require "right_aws"
  require "sdb/active_sdb"
  require "hbacker"
  require "hbacker/stargate"
  require "pp"
  
  class TableInfo < RightAws::ActiveSdb::Base
    columns do
      name
      start_time  :Integer
      end_time  :Integer
      max_versions :Integer
      versions :Integer
      compression
      in_memory :Boolean
      block_cache :Boolean
      blockcache :Boolean
      blocksize :Integer
      length :Integer
      ttl :Integer
      bloomfilter
      created_at :DateTime
    end
  end
  
  class Db 
    # Initializes SimpleDB Table and connection
    # @param [String] aws_access_key_id Amazon Access Key ID
    # @param [String] aws_secret_access_key Amazon Secret Access Key
    # @param [String] hbase_name Name to refer to the HBase cluster by
    #   Usually the FQDN with dots turned to underscores
    #
    def initialize(aws_access_key_id, aws_secret_access_key, hbase_name)
      table_name = "#{hbase_name}_table_info"
      
      # This seems to be the only way to dynmaically set the domain name
      @table_class = Class.new(TableInfo) { set_domain_name table_name}
      
      # connect to SDB
      RightAws::ActiveSdb.establish_connection(aws_access_key_id, aws_secret_access_key, :logger => Hbacker.log)

      # Creating a domain is idempotent. Its easier to try to create than to check if it already exists
      @table_class.create_domain
    end
    
    # Records HBase Table Info into SimpleDB table
    # @param [String] table_name Name of the HBase Table
    # @param [Integer] start_time Earliest Time to backup from (milliseconds since Unix Epoch)
    # @param [Integer] end_time Latest Time to backup to (milliseconds since Unix Epoch)
    # @param [Stargate::Model::TableDescriptor] table_descriptor Schema of the HBase Table
    # @param [Integer] versions Max number of row/cell versions to backup
    #
    def record_table_info(table_name, start_time, end_time, table_descriptor, versions)
      table_descriptor.column_families_to_hashes.each do |column|
        @table_class.create(column.merge({:table_name => table_name, :start_time => start_time, :end_time => end_time}))
      end
    end
  end
end