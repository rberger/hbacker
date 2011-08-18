

module Hbacker
  require "mysql"
  require 'rubygems'  
  require 'active_record'

  ActiveRecord::Base.logger = Logger.new(STDOUT)
  
  ## --- Model ---
  
  class HbackerSession < ActiveRecord::Base
    has_many :hbase_tables, :dependent => :destroy
  end
  
  class HbaseTable < ActiveRecord::Base
    belongs_to :hbacker_session
    has_many   :column_descriptors, :dependent => :destroy
    cattr_accessor :hbase_name
  end
  
  class ColumnDescriptor < ActiveRecord::Base
    belongs_to :hbase_table
  end
  
  ## --- Migrations ---
  
  class MigrateHbackerSession < ActiveRecord::Migration
    def self.up
      create_table :hbacker_sessions do |t|
        t.string   :mode
        t.string   :cluster_name
        t.string   :session_name
        t.string   :dest_root
        t.integer  :specified_start
        t.integer  :specified_end
        t.datetime :started_at
        t.datetime :ended_at, :default  => Time.at(0) # lambda { Time.at(0) }
        t.boolean  :error
        t.string   :error_info
        t.datetime :updated_at
        t.datetime :created_at, :default => Time.now.utc # lambda{ Time.now.utc }
      end
    end
    
    def self.down
      drop_table :hbacker_sessions
    end
  end
  
  class MigrateHbaseTable < ActiveRecord::Migration
    def self.up
      create_table :hbase_tables do |t|
        t.string   :mode
        t.string   :table_name
        t.string   :session_name
        t.integer  :start_time
        t.integer  :end_time
        t.integer  :specified_versions
        t.boolean  :empty
        t.boolean  :error
        t.string   :error_info
        t.datetime :created_at, :default => Time.now.utc # lambda{ Time.now.utc }
        t.datetime :updated_at
        t.references :hbacker_session
        #t.integer  :hbacker_session_id
      end
    end
    
    def self.down
      drop_table :hbase_tables
    end
  end
  
  class MigrateColumnDescriptor < ActiveRecord::Migration
    def self.up
      create_table :column_descriptors do |t|
        t.string   :mode
        t.string   :session_name
        t.string   :table_name
        t.string   :name
        t.string   :blockcache
        t.integer  :blocksize
        t.string   :bloomfilter
        t.string   :compression
        t.boolean  :block_cache
        t.integer  :max_versions
        t.boolean  :in_memory
        t.integer  :versions
        t.integer  :length
        t.integer  :ttl
        t.datetime :updated_at
        t.datetime :created_at, :default => Time.now.utc # lambda{ Time.now.utc }
        t.references :hbase_table
        #t.integer  :hbase_table_id
      end
    end
    
    def self.down
      drop_table :column_descriptors
    end
  end

  ## -- Main code ---
  class Db
    attr_reader :db_config, :hbase_name, :aws_access_key_id, :aws_secret_access_key
    def initialize(mode, db_config, hbase_name, aws_access_key_id, aws_secret_access_key, reiteration_time=5)
      @mode = mode
      @db_config = db_config
      @aws_access_key_id = aws_access_key_id
      @aws_secret_access_key = aws_secret_access_key
      @hbase_name = hbase_name
      @db_count ||= 0
      @db_count += 1
      
      Db.create_connection(db_config)
      
      #hostport = @db_config[:hostport] || "localhost"
      #database = @db_config[:database] || "hbacker"
      #username = @db_config[:username] || "root"
      #password = @db_config[:password] || ""
      #
      #puts "========================"
      #puts "Connecting to MySQL hostport=#{hostport}, username=#{username}, password=#{password}, database=#{database}"
      #puts "========================"
      #Mysql.real_connect(hostport, username, password, database, 3306, "/tmp/mysql.sock")
      #ActiveRecord::Base.establish_connection( :adapter=> "mysql",  
      #                                         :host => hostport,
      #                                         :username => username,
      #                                         :password => password,  
      #                                         :database=> database )

      # if @mode == :export
      #   create_export_table_classes(@hbase_name)
      # else
      #   create_import_table_classes(@hbase_name)
      # end
      #       
    end
    
    class DbError < HbackerError ; end
    
    # Records Exported HBase Table Info into SimpleDB table
    # @param [String] table_name Name of the HBase Table
    # @param [Integer] start_time Earliest Time to export from (milliseconds since Unix Epoch)
    # @param [Integer] end_time Latest Time to export to (milliseconds since Unix Epoch)
    # @param [Stargate::Model::TableDescriptor] table_descriptor Schema of the HBase Table
    # @param [Integer] versions Max number of row/cell versions to export
    # @param [String] session_name Name (usually the date_time_stamp) of the export session
    # @param [Boolean] empty True if the table is totally empty
    # @param [Boolean] error True if there was a hard error while doing the operation
    # @param [String] error_info Basic info about the error if error is true
    #
    def exported_table_info(table_name, start_time, end_time, table_descriptor, versions, session_name, empty=false, error={})
      now = Time.now.utc
      table_info = {
        :mode         => "export",
        :table_name   => table_name,
        :start_time   => start_time,
        :end_time     => end_time,
        :session_name => session_name,
        :empty        => empty,
        :error        => error.empty? ? false : true,
        :error_info   => error.empty? ? nil : error[:info],
        :specified_versions => versions,
        :updated_at   => now
      }
      session = HbackerSession.where(:session_name => session_name)
      raise DbError, "No record with session_name #{session_name}" if session.empty?

      table = session.shift.hbase_tables.create(table_info)
      #table = session.shift.hbase_tables << table_info
      #----
      #sid = session.shift
      #puts "Session ID", sid
      #table = HbaseTable.new(table_info.merge("hbacker_session_id" => sid))
      #table.save
      #----
      #ExportedHbaseTable.create(table_info)

      puts "%%%%%%%%%%"
      puts table_descriptor.inspect
      puts "%%%%%%%%%%"
      if table_descriptor
        table_descriptor.column_families_to_hashes.each do |column|
          column.merge!({ :table_name   => table_name, 
                          :session_name => session_name,
                          :updated_at   => now })
          table.column_descriptors.create(column)
          #ExportedColumnDescriptor.create(column)
        end
      end
    end
    
    
    # Records Imported HBase Table Info into SimpleDB table
    # @param [String] table_name Name of the HBase Table
    # @param [String] session_name Name (usually the date_time_stamp) of the export session
    # @param [Boolean] empty True if the table is totally empty
    # @param [Boolean] error True if there was a hard error while doing the operation
    # @param [String] error_info Basic info about the error if error is true
    #
    def imported_table_info(table_name, session_name, empty=false, error={})
      now = Time.now.utc
      table_info = {
        :mode         => "import",
        :table_name   => table_name,
        :session_name => session_name,
        :empty        => empty,
        :error        => error.empty? ? false : true,
        :error_info   => error.empty? ? nil : error[:info],
        :updated_at   => now
      }
      session = HbackerSession.where(:session_name => session_name)
      raise DbError, "No record with session_name #{session_name}" unless session
      
      table = session.hbase_tables.create(table_info)
      
      if table_descriptor
        table_descriptor.column_families_to_hashes.each do |column|
          column.merge!({ :table_name   => table_name, 
                          :session_name => session_name,
                          :updated_at   => now })
          table.column_descriptors.create(column)
          #ImportedColumnDescriptor.create(column)
        end
      end
    end


    # Records the begining of a export session
    # @param [String] session_name Name (usually the date_time_stamp) of the export session
    # @param [String] dest_root The scheme and root path of where the export is put
    # @param [Integer] specified_start The start_time of the earliest record to be backed up.
    #   Value of 0 means its a full export
    # @param [Integer] specified_end End time of the last record to be backed up
    # @param [Time] started_at When the export started
    #
    def start_info(session_name, dest_root, specified_start, specified_end, started_at)
      session_info = {
        :mode            => @mode,
        :session_name    => session_name, 
        :specified_start => specified_start,
        :specified_end   => specified_end,
        :started_at      => started_at, 
        :dest_root       => dest_root, 
        :cluster_name    => @hbase_name,
        :updated_at      => Time.now.utc
      }

      HbackerSession.create(session_info)
    end
    
    # Records the end of a export session (Updates existing record)
    # @param [String] session_name Name (usually the date_time_stamp) of the export session
    # @param [Time] ended_at When the export ended
    # @param [String] dest_root The scheme and root path of where the export is put
    #
    def end_info(session_name, dest_root, ended_at, error={})
      now = Time.now.utc
      Hbacker.log.debug "end_info(@mode: #{@mode} @hbase_name: #{@hbase_name.inspect}session_name: #{session_name.inspect}, dest_root: #{dest_root.inspect}, ended_at: #{ended_at.inspect}, error: #{error.inspect}"
      
      # Loop prevents a race condition of the start_info call update not being complete before the end_info call
      count = 0
      info = nil
      while (info = HbackerSession.where(:mode => @mode, :cluster_name => @hbase_name, :session_name => session_name, :dest_root => dest_root)).nil? && count < 10
        sleep 3
        count += 1
      end
      raise DbError, "HbackerSession.where(:cluster_name => #{@hbase_name}, :session_name => #{session_name}, :dest_root => #{dest_root}) is nil" if info.nil?
      
      info.reload
      Hbacker.log.debug "end_info(session_name: #{session_name.inspect}, dest_root: #{dest_root.inspect}, ended_at: #{ended_at.inspect}, error: #{error.inspect}"
      puts "=======OBject id"
      puts info.inspect
      HbackerSession.update(info.first.id, :error      => error.empty? ? false : true,
                            :error_info => error.empty? ? nil : error[:info],
                            :ended_at   => ended_at,
                            :updated_at => now )
    end
  
    # Returns a list of names of tables backed up during the specified session
    # @param [String] session_name Name (usually the date_time_stamp) of the export session
    # @param [String] dest_root The scheme and root path of where the export is put
    # @return [Array<String>] List of table namess that were backed up for specified session
    #
    def table_names(session_name, dest_root, table_name=nil)
      if table_name && table_name.include?("%")
        session_name = @@hbase_session
        hbase_session = HbaseSession.where(:session_name => session_name).first
        conditions = ['mode = ? AND table_name like ? AND session_name = ?', @mode, table_name, session_name]
      else
        conditions = ['mode = ? AND session_name = ?', @mode, session_name]
      end
      Hbacker.log.debug "Db(mysql.rb)/table_names/conditions: #{conditions.inspect}"
      cond_result = HbaseTable.where(conditions)
      Hbacker.log.debug "Cond result rows: #{cond_result.inspect}"
      results = cond_result.all.select{|table| table.hbase_session.dest_root == dest_root }.collect do |t|
        t.reload
        t[:table_name]
      end
    end
    
    # Returns a list of info for tables backed up during the specified session
    # @param [String] session_name Name (usually the date_time_stamp) of the export session
    # @param [String] dest_root The scheme and root path of where the export is put
    # @param [String] table_name If specified, only the table name selected will be returnd.
    #   % can be used as a wildcard at begining and/or end
    # @return [Array<Hash>] List of table info that were backed up for specified session
    #
    def list_table_info(session_name, dest_root, table_name=nil)
      puts "@@@@@@@@@@@@@@@@@@@@@@@@@@@@"
      if table_name && table_name.include?("%")
        conditions = ['mode = ? AND table_name like ? AND session_name = ? AND dest_root = ?', @mode, table_name, session_name, dest_root]
      else
        conditions = ['mode = ? AND session_name = ? AND dest_root = ?', @mode, session_name, dest_root]
      end 
      puts "@@@@@@@@@@@@@@@@@@@@@@@@@@@@"     
      results = HbaseTable.where(conditions).allcollect do |t|
        t.reload
        t.attributes
      end

        puts "entered"
        puts "@@@@@@@@@@@@@@@@@@@@@@@@@@@@"

    end
    
    ##
    # Get the Attributes of an HBase table previously recorded ColumnDescriptor Opts
    # @param [String] table_name The name of the HBase table 
    # @param (see #table_names)
    # @return [Hash] The hash of attributes found
    #
    def column_descriptors(table_name, session_name)
      results = {}

      #TODO: find what is `k` and replace with MySQL calls
      ColumnDescriptor.where(:mode => @mode, :session => session_name, :table => table_name).each do |t|
        t.reload
        t.each_pair do |k,v|
          results.merge(k.to_sym => v) if Stargate::Model::ColumnDescriptor.AVAILABLE_OPTS[k]
        end
      end
      results
    end

    # Returns a list of info for exports for the specified session
    # @param [Symbol] mode :export | :import
    # @param [String] session_name Name (usually the date_time_stamp) of the export session
    #   % can be used as a wildcard at begining and/or end
    # @return [Array<Hash>] List of export info that were backed up for specified session
    #
    def session_info(mode, session_name)
      if session_name && session_name.include?("%")
        conditions = {:conditions  => ["mode = ? AND session_name like ?", mode, session_name]}
      elsif session_name
        conditions = {:conditions  => ["mode = ? AND session_name = ?", mode, session_name]}
      else
        conditions = nil
      end

      Hbacker.log.debug "conditions: #{conditions.inspect}"
      results = HbackerSession.select(:all, conditions)
      Hbacker.log.debug "results: #{results.inspect}"
      results.collect do |session_info|
        Hbacker.log.debug "session_info: #{session_info.inspect}"
        session_info.reload
        session_info.attributes
      end
    end
    # --------------------------------------------

    def self.create_connection(db)
      puts ">>>>>>>>>>>>>>>>"
      puts db.inspect
      puts ">>>>>>>>>>>>>>>>"
      ActiveRecord::Base.establish_connection( :adapter  => db['adapter'],
                                               :host     => db['host'],
                                               :username => db['username'],
                                               :password => db['password'],
                                               :database => db['database'] )
    end
    
    def self.create_export_table_classes()
      #HbackerSession.hbase_name = hbase_name

      begin 
      MigrateHbackerSession.down
      rescue Exception => e
      end
      begin 
      MigrateHbaseTable.down
      rescue Exception => e
      end
      begin 
      MigrateColumnDescriptor.down
      rescue Exception => e
      end
      
      MigrateHbackerSession.up
      MigrateHbaseTable.up
      MigrateColumnDescriptor.up
    end

    def self.create_import_table_classes()
      create_export_table_classes
    end
    
  end
end
