module Hbacker
  require "hbacker"
  require "pp"
  
  class Export
    ##
    # Initialize the Export Instance
    #
    def initialize(hbase, db, hbase_home, hbase_version, hadoop_home)
      @hbase = hbase
      @db = db
      @hadoop_home = hadoop_home
      @hbase_home = hbase_home
      @hbase_version = hbase_version
    end

    ##
    # Querys HBase to get a list of all the tables in the cluser
    # Iterates thru the list calling Export#table to do the Export to the specified dest
    #
    def all_tables(opts)
      Hbacker.log.debug "Export#all_tables"
      @hbase.list_tables.each do |table|
        dest = "#{opts[:destination]}#{opts[:backup_timestamp]}/#{table.name}/"
        Hbacker.log.info "Backing up #{table.name} to #{dest}"
        Hbacker.log.debug "self.table(#{table.name},#{ opts[:start]}, #{opts[:end]}, #{dest}, #{opts[:versions]})"
        self.table(table.name, opts[:start], opts[:end], dest, opts[:versions])
      end
    end
    
    ##
    # Iterates thru the list of tables calling Export#table to do the Export to the specified dest
    def specified_tables(opts)
      opts[:tables].each do |table|
        dest = "#{opts[:destination]}#{opts[:backup_timestamp]}/#{table}/"
        Hbacker.log.info "Backing up #{table} to #{dest}"
        Hbacker.log.debug "self.table(#{table},#{ opts[:start]}, #{opts[:end]}, #{dest}, #{opts[:versions]})"
        self.table(table, opts[:start], opts[:end], dest, opts[:versions])
      end
    end
    
    ##
    # * Uses Hadoop to export specfied table from HBase to destination file system
    # * Record that the date range and schema of table was exported to
    # 
    def table(table_name, start_time, end_time, destination, versions)
      table_descriptor = @hbase.table_descriptor(table_name)
      
      cmd = "#{@hadoop_home}/bin/hadoop jar #{@hbase_home}/hbase-#{@hbase_version}.jar export " +
        "#{table_name} #{destination} #{versions} #{start_time} #{end_time}"
      Hbacker.log.debug "About to execute #{cmd}"
      cmd_output = `#{cmd} 2>&1`
      # Hbacker.log.debug "cmd output: #{cmd_output}"
      
      Hbacker.log.debug "$?.exitstatus: #{$?.exitstatus.inspect}"
      
      if $?.exitstatus > 0
        Hbacker.log.error"Hadoop command failed:"
        Hbacker.log.error cmd_output
        exit(-1)
      end
      @db.record_table_info(table_name, start_time, end_time, table_descriptor, versions)
    end
  end
end
