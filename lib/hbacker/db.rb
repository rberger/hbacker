module Hbacker
  require "right_aws"
  require "sdb/active_sdb"
  require "hbacker"

  class Db
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
      
      create_table_classes
      
      # connect to SDB
      RightAws::ActiveSdb.establish_connection(aws_access_key_id, aws_secret_access_key, :logger => Hbacker.log)

      # Creating a domain is idempotent. Its easier to try to create than to check if it already exists
      BackupSession.create_domain
      BackedupHbaseTable.create_domain
      BackedupColumnDescriptor.create_domain
      RestoreSession.create_domain
      RestoredHbaseTable.create_domain
      RestoredHColumnDescriptors.create_domain
    end
    
    # Records HBase Table Info into SimpleDB table
    # @param [String] table_name Name of the HBase Table
    # @param [Integer] start_time Earliest Time to backup from (milliseconds since Unix Epoch)
    # @param [Integer] end_time Latest Time to backup to (milliseconds since Unix Epoch)
    # @param [Stargate::Model::TableDescriptor] table_descriptor Schema of the HBase Table
    # @param [Integer] versions Max number of row/cell versions to backup
    # @param [String] session_name Name (usually the date_time_stamp) of the backup session
    #
    def table_backup_info(table_name, start_time, end_time, table_descriptor, versions, session_name, empty, error=false)
      now = Time.now.utc
      table_backup_info = {
        :table_name => table_name, 
        :session_name => session_name,
        :empty => empty.inspect,
        :error => error.inspect,
        :specified_versions => versions,
        :updated_at => now
      }
      table_descriptor.column_families_to_hashes.each do |column|
        added_info = {
          :table_name => table_name, 
          :session_name => session_name,
          :updated_at => now
        }
        
        info = column_info.merge(added_info)
        # Hbacker.log.debug "column: #{column.inspect}"
        # Hbacker.log.debug "added_info: #{added_info.inspect}"
        # Hbacker.log.debug "saved_info: #{info.inspect}"
        BackedupColumnDescriptor.create(info)
      end
    end
  
    # Records the begining of a backup session
    # @param [String] session_name Name (usually the date_time_stamp) of the backup session
    # @param [String] dest_root The scheme and root path of where the backup is put
    # @param [Integer] specified_start The start_time of the earliest record to be backed up.
    #   Value of 0 means its a full backup
    # @param [Integer] specified_end End time of the last record to be backed up
    # @param [Time] session_started_at When the backup started
    #
    def backup_start_info(session_name, dest_root, specified_start, specified_end, session_started_at)
      BackupSession.create(
        {
          :session_name => session_name, 
          :specified_start => specified_start,
          :specified_end => specified_end,
          :session_started_at => session_started_at, 
          :dest_root => dest_root, 
          :cluster_namee => @hbase_name,
          :updated_at => Time.now.utc
        }
      )
    end
  
    # Records the end of a backup session (Updates existing record)
    # @param [String] session_name Name (usually the date_time_stamp) of the backup session
    # @param [Time] ended_at When the backup ended
    # @param [String] dest_root The scheme and root path of where the backup is put
    #
    def backup_end_info(session_name, dest_root, ended_at, error=nil, error_info=nil)
      now = Time.now.utc
      info = BackupSession.find_by_name_and_dest_root(session_name, dest_root)
      info.reload
      info[:error] = error if error
      info[:error_info] = error_info if error_info
      info[:ended_at] = ended_at
      info[:updated_at] = now
      info.save
    end
  
    # Returns a list of names of tables backed up during the specified session
    # @param [String] session_name Name (usually the date_time_stamp) of the backup session
    # @param [String] dest_root The scheme and root path of where the backup is put
    # @return [Array<String>] List of table namess that were backed up for specified session
    #
    def backup_table_names(session_name, dest_root, table_name=nil)
      if table_name && table_name.include?("%")
        conditions = ['table_name like ? AND session_name = ? AND dest_root = ?', table_name, session_name, dest_root]
      else
        conditions = ['session_name = ? AND dest_root = ?', session_name, dest_root]
      end
      results = BackedupHbaseTable.select(:all, :conditions => conditions).collect do |t|
        t.reload
        t[:table_name]
      end
    end
    
    # Returns a list of info for tables backed up during the specified session
    # @param [String] session_name Name (usually the date_time_stamp) of the backup session
    # @param [String] dest_root The scheme and root path of where the backup is put
    # @param [String] table_name If specified, only the table name selected will be returnd.
    #   % can be used as a wildcard at begining and/or end
    # @return [Array<Hash>] List of table info that were backed up for specified session
    #
    def backup_table_info(session_name, dest_root, table_name=nil)
      if table_name && table_name.include?("%")
        conditions = ['table_name like ? AND session_name = ? AND dest_root = ?', table_name, session_name, dest_root]
      else
        conditions = ['session_name = ? AND dest_root = ?', session_name, dest_root]
      end
      results = BackedupHbaseTable.select(:all, :conditions => conditions).collect do |t|
        t.reload
        t.attributes
      end
    end
    
    ##
    # Get the Attributes of an HBase table previously recorded ColumnDescriptor Opts
    # @param [String] table_name The name of the HBase table 
    # @param (see #backup_table_names)
    # @return [Hash] The hash of attributes found
    #
    def column_descriptors(table_name, session_name, dest_root)
      results = {}
      BackedupColumnDescriptor.find_all_by_session_name_and_dest_root_and_table_name(session_name, dest_root, table_name).each do |t|
        t.reload
        t.each_pair do |k,v|
          results.merge(k.to_sym => v) if Stargate::Model::ColumnDescriptor.AVAILABLE_OPTS[k]
        end
      end
      results
    end

    # Returns a list of info for backups for the specified session
    # @param [String] session_name Name (usually the date_time_stamp) of the backup session
    #   % can be used as a wildcard at begining and/or end
    # @return [Array<Hash>] List of backup info that were backed up for specified session
    #
    def backup_info(session_name)
      if session_name && session_name.include?("%")
        conditions = {:conditions  => ["session_name like ?", session_name]}
      elsif session_name
        conditions = {:conditions  => ["session_name = ?", session_name]}
      else
        conditions = nil
      end
      BackupSession.select(:all, conditions).collect do |backup_info|
        backup_info.reload
        backup_info.attributes
      end
    end
    
    private
    def create_table_classes
      # Dynmaically create Class so we can dynamically set the name of the "Domain" in SimpleDB
      
        # Top level record of a backup session
        # One SimpleDB table for all Backups 
        # (cluster_name specifies the HBase Cluster backed up)
        # One record per backup session
        Object::const_set('BackupSession',  Class.new(RightAws::ActiveSdb::Base) do
          set_domain_name "backup_info"
          columns do
            cluster_name
            session_name
            dest_root
            specified_start :Integer
            specified_end :Integer
            started_at :DateTime
            ended_at :DateTime
            error :Boolean
            error_info
            updated_at :DateTime
            created_at :DateTime, :default => lambda{ Time.now.utc }
          end
        end
        )

        # Top level record of a restore session
        # One SimpleDB table for all Restores 
        # (cluster_name specifies the HBase Cluster restored)
        # One record per restore session
        Object::const_set('RestoreSession',  Class.new(RightAws::ActiveSdb::Base) do
          set_domain_name "restore_info"
          columns do
            cluster_name
            restore_name
            source_root
            specified_start :Integer
            specified_end :Integer
            started_at :DateTime
            ended_at :DateTime
            error :Boolean
            error_info
            updated_at :DateTime
            created_at :DateTime, :default => lambda{ Time.now.utc }
          end
        end
        )

      # Records the status of each HBase Table backed up
      # There is a SimpleDb Domain for each HBase Cluster backed up
      # Each row represents the state of an Hbase Table backup
      Object::const_set('BackedupHbaseTable', Class.new(RightAws::ActiveSdb::Base) do
        set_domain_name "backedup_#{hbase_name}_tables"
        columns do
          table_name
          session_name
          start_time  :Integer
          end_time  :Integer
          specified_versions :Integer
          empty :Boolean
          error :Boolean
          created_at :DateTime, :default => lambda{ Time.now.utc }
          updated_at :DateTime
        end
      end
      )

      # Records Column Family Descriptions for each Table backed up
      # There is a SimpleDb Domain forfor each HBase Cluster backed up
      # Each row represents a Column Family of an HBase Table
      # There can be multple rows (multiple Column Families) for each HBase Table
      Object::const_set('BackedupColumnDescriptor', Class.new(RightAws::ActiveSdb::Base) do
        set_domain_name "backedup_#{hbase_name}_column_descriptors"
        columns do
          session_name
          table_name
          name
          blockcache
          blocksize :Integer
          bloomfilter
          compression
          block_cache :Boolean
          max_versions :Integer
          in_memory :Boolean
          versions :Integer
          length :Integer
          ttl :Integer
          updated_at :DateTime
          created_at :DateTime, :default => lambda{ Time.now.utc }
        end
      end
      )
      
      # Records the status of each HBase Table restored
      # There is a SimpleDb Domain for each HBase Cluster restored
      # Each row represents the state of an Hbase Table restore
      Object::const_set('RestoredHbaseTable', Class.new(RightAws::ActiveSdb::Base) do
        set_domain_name "restored_#{hbase_name}_tables"
        columns do
          table_name
          session_name
          start_time  :Integer
          end_time  :Integer
          specified_versions :Integer
          empty :Boolean
          error :Boolean
          created_at :DateTime, :default => lambda{ Time.now.utc }
          updated_at :DateTime
        end
      end
      )

      # Records Column Family Descriptions for each Table restored
      # There is a SimpleDb Domain forfor each HBase Cluster restored
      # Each row represents a Column Family of an HBase Table
      # There can be multple rows (multiple Column Families) for each HBase Table
      Object::const_set('RestoredColumnDescriptor', Class.new(RightAws::ActiveSdb::Base) do
        set_domain_name "restored_#{hbase_name}_column_descriptors"
        columns do
          session_name
          table_name
          name
          blockcache
          blocksize :Integer
          bloomfilter
          compression
          block_cache :Boolean
          max_versions :Integer
          in_memory :Boolean
          versions :Integer
          length :Integer
          ttl :Integer
          updated_at :DateTime
          created_at :DateTime, :default => lambda{ Time.now.utc }
        end
      end
      )
    end
  end
end