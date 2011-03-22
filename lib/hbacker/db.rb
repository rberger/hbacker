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
    def initialize(aws_access_key_id, aws_secret_access_key, hbase_name)
      table_name = "#{hbase_name}_table_info"
      
      # This seems to be the only way to dynmaically set the domain name
      @table_class = Class.new(TableInfo) { set_domain_name table_name}
      
      # connect to SDB
      RightAws::ActiveSdb.establish_connection(aws_access_key_id, aws_secret_access_key, :logger => Hbacker.log)

      # Creating a domain is idempotent. Its easier to try to create than to check if it already exists
      @table_class.create_domain
    end
    
    def record_table_info(table_name, start_time, end_time, table_descriptor, versions)
      table_descriptor.column_families_to_hashes.each do |column|
        @table_class.create(column.merge({:table_name => table_name, :start_time => start_time, :end_time => end_time}))
      end
    end
  end
end