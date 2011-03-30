module Hbacker
  require "right_aws"
  require "sdb/active_sdb"
  require "hbacker"
  
  class TableInfo < RightAws::ActiveSdb::Base
    columns do
      table_name
      name
      start_time  :Integer
      end_time  :Integer
      max_versions :Integer
      versions :Integer
      specified_versions :Integer
      compression
      in_memory :Boolean
      block_cache :Boolean
      blockcache :Boolean
      blocksize :Integer
      length :Integer
      ttl :Integer
      bloomfilter
      backup_name
      empty :Boolean
      error :Boolean
      created_at :DateTime, :default => lambda{ Time.now.utc }
      updated_at :DateTime
    end
  end
  
  class BackupInfo < RightAws::ActiveSdb::Base
    columns do
      backup_name
      specified_start :Integer
      specified_end :Integer
      started_at :DateTime
      ended_at :DateTime
      dest_root
      domain_name
      created_at :DateTime, :default => lambda{ Time.now.utc }
      updated_at :DateTime
    end
  end
  
  class RestoreInfo < RightAws::ActiveSdb::Base
    columns do
      restore_name
      specified_start :Integer
      specified_end :Integer
      started_at :DateTime
      ended_at :DateTime
      source_root
      domain_name
      created_at :DateTime, :default => lambda{ Time.now.utc }
      updated_at :DateTime
    end
  end
  
  class Db
    attr_reader :aws_access_key_id, :aws_secret_access_key, :hbase_name, 
                :backup_info_class, :hbase_table_info_class
      
    # Initializes SimpleDB Table and connection
    # @param [String] aws_access_key_id Amazon Access Key ID
    # @param [String] aws_secret_access_key Amazon Secret Access Key
    # @param [String] hbase_name Name to refer to the HBase cluster by
    #   Usually the FQDN with dots turned to underscores
    #
    def initialize(aws_access_key_id, aws_secret_access_key, hbase_name)
      @aws_access_key_id = aws_access_key_id
      @aws_secret_access_key =aws_secret_access_key
      @hbase_name = hbase_name
      
      # This seems to be the only way to dynmaically set the domain name
      @hbase_table_info_class = Class.new(TableInfo) { set_domain_name "#{hbase_name}_table_info" }
      # And had to do BackupInfo this way as the right_aws library was trying to use Hbacker::BackupInfo 
      @backup_info_class =  Class.new(BackupInfo) { set_domain_name "backup_info" }
      
      # connect to SDB
      RightAws::ActiveSdb.establish_connection(aws_access_key_id, aws_secret_access_key, :logger => Hbacker.log)

      # Creating a domain is idempotent. Its easier to try to create than to check if it already exists
      @hbase_table_info_class.create_domain
      @backup_info_class.create_domain
    end
    
    # Records HBase Table Info into SimpleDB table
    # @param [String] table_name Name of the HBase Table
    # @param [Integer] start_time Earliest Time to backup from (milliseconds since Unix Epoch)
    # @param [Integer] end_time Latest Time to backup to (milliseconds since Unix Epoch)
    # @param [Stargate::Model::TableDescriptor] table_descriptor Schema of the HBase Table
    # @param [Integer] versions Max number of row/cell versions to backup
    # @param [String] backup_name Name (usually the date_time_stamp) of the backup session
    #
    def record_table_info(table_name, start_time, end_time, table_descriptor, versions, backup_name, empty, error=false)
      table_descriptor.column_families_to_hashes.each do |column|
        added_info = {
          :table_name => table_name, 
          :start_time => start_time, 
          :end_time => end_time,
          :specified_versions => versions,
          :backup_name => backup_name,
          :empty => empty.inspect,
          :error => error.inspect,
          :updated_at => Time.now.utc
        }
        
        # ActiveSdb doesn't seem to be handling booleans right so we convert them to strings
        column_info = column.inject({}) do  |h, (k, v)|
          if v.nil?
            v
          elsif [TrueClass, FalseClass].include?(v.class)
            v = v.to_s
          end
          h.merge(k => v)
        end
        
        info = column_info.merge(added_info)
        # Hbacker.log.debug "column: #{column.inspect}"
        # Hbacker.log.debug "added_info: #{added_info.inspect}"
        # Hbacker.log.debug "saved_info: #{info.inspect}"
        @hbase_table_info_class.create(info)
      end
    end
  
    # Records the begining of a backup session
    # @param [String] backup_name Name (usually the date_time_stamp) of the backup session
    # @param [String] dest_root The scheme and root path of where the backup is put
    # @param [Integer] specified_start The start_time of the earliest record to be backed up.
    #   Value of 0 means its a full backup
    # @param [Integer] specified_end End time of the last record to be backed up
    # @param [Time] session_started_at When the backup started
    #
    def record_backup_start(backup_name, dest_root, specified_start, specified_end, session_started_at)
      @backup_info_class.create(
        {
          :backup_name => backup_name, 
          :specified_start => specified_start,
          :specified_end => specified_end,
          :session_started_at => session_started_at, 
          :dest_root => dest_root, 
          :domain_name => @hbase_table_info_class.domain,
          :updated_at => Time.now.utc
        }
      )
    end
  
    # Records the end of a backup session (Updates existing record)
    # @param [String] backup_name Name (usually the date_time_stamp) of the backup session
    # @param [Time] ended_at When the backup ended
    # @param [String] dest_root The scheme and root path of where the backup is put
    #
    def record_backup_end(backup_name, dest_root, ended_at)
      info = @backup_info_class.find_by_name_and_dest_root(backup_name, dest_root)
      info.reload
      info[:ended_at] = ended_at
      info[:updated_at] = Time.now.utc
      info.save
    end
  
    # Returns a list of names of tables backed up during the specified session
    # @param [String] backup_name Name (usually the date_time_stamp) of the backup session
    # @param [String] dest_root The scheme and root path of where the backup is put
    # @return [Array<String>] List of table namess that were backed up for specified session
    #
    def table_names(backup_name, dest_root, table_name=nil)
      if table_name && table_name.include?("%")
        conditions = ['table_name like ? AND backup_name = ? AND dest_root = ?', table_name, backup_name, dest_root]
      else
        conditions = ['backup_name = ? AND dest_root = ?', backup_name, dest_root]
      end
      results = @hbase_table_info_class.select(:all, :conditions => conditions).collect do |t|
        t.reload
        t[:table_name]
      end
    end
    
    # Returns a list of info for tables backed up during the specified session
    # @param [String] backup_name Name (usually the date_time_stamp) of the backup session
    # @param [String] dest_root The scheme and root path of where the backup is put
    # @param [String] table_name If specified, only the table name selected will be returnd.
    #   % can be used as a wildcard at begining and/or end
    # @return [Array<Hash>] List of table info that were backed up for specified session
    #
    def table_info(backup_name, dest_root, table_name=nil)
      if table_name && table_name.include?("%")
        conditions = ['table_name like ? AND backup_name = ? AND dest_root = ?', table_name, backup_name, dest_root]
      else
        conditions = ['backup_name = ? AND dest_root = ?', backup_name, dest_root]
      end
      results = @hbase_table_info_class.select(:all, :conditions => conditions).collect do |t|
        t.reload
        t.attributes
      end
    end
    
    ##
    # Get the Attributes of an HBase table previously recorded ColumnDescriptor Opts
    # @param [String] table_name The name of the HBase table 
    # @param (see #table_names)
    # @return [Hash] The hash of attributes found
    #
    def table_attributes(table_name, backup_name, dest_root)
      results = {}
      @hbase_table_info_class.find_all_by_backup_name_and_dest_root_and_table_name(backup_name, dest_root, table_name).each do |t|
        t.reload
        t.each_pair do |k,v|
          results.merge(k.to_sym => v) if Stargate::Model::ColumnDescriptor.AVAILABLE_OPTS[k]
        end
      end
      results
    end

    # Returns a list of info for backups for the specified session
    # @param [String] backup_name Name (usually the date_time_stamp) of the backup session
    #   % can be used as a wildcard at begining and/or end
    # @return [Array<Hash>] List of backup info that were backed up for specified session
    #
    def backup_info(backup_name)
      if backup_name && backup_name.include?("%")
        conditions = {:conditions  => ["backup_name like ?", backup_name]}
      elsif backup_name
        conditions = {:conditions  => ["backup_name = ?", backup_name]}
      else
        conditions = nil
      end
      @backup_info_class.select(:all, conditions).collect do |backup_info|
        backup_info.reload
        backup_info.attributes
      end
    end
  end
end