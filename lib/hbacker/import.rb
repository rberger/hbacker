module Hbacker
  require "hbacker"
  require "pp"
  
  class Import
    ##
    # Initialize the Import Instance
    #
    def initialize(hbase, db, hbase_home, hbase_version, hadoop_home, s3)
      @hbase = hbase
      @db = db
      @hadoop_home = hadoop_home
      @hbase_home = hbase_home
      @hbase_version = hbase_version
      @s3 = s3
    end

    ##
    # Querys HBase to get a list of all the tables in the cluser
    # Iterates thru the list calling Import#table to do the Import to the specified dest
    #
    def all_tables(opts)
      Hbacker.log.debug "Import#all_tables from #opts[:source] to #{@hbase_home}"
      @hbase.list_tables.each do |table|
        dest = "#{opts[:destination]}#{opts[:backup_timestamp]}/#{table.name}/"
        Hbacker.log.info "Backing up #{table.name} to #{dest}"
        Hbacker.log.debug "self.table(#{table.name},#{ opts[:start]}, #{opts[:end]}, #{dest}, #{opts[:versions]})"
        self.table(table.name, opts[:start], opts[:end], dest, opts[:versions])
      end
    end
    
    ##
    # Iterates thru the list of tables calling Import#table to do the Import to the specified dest
    # * Get the list of names based on the 
    def specified_tables(opts)
      opts[:tables].each do |table|
        dest = "#{opts[:destination]}#{opts[:backup_timestamp]}/#{table}/"
        Hbacker.log.info "Backing up #{table} to #{dest}"
        Hbacker.log.debug "self.table(#{table},#{ opts[:start]}, #{opts[:end]}, #{dest}, #{opts[:versions]})"
        self.table(table, opts[:start], opts[:end], dest, opts[:versions])
      end
    end
    
    ##
    # * Uses Hadoop to import specfied table from HBase to destination file system
    # * Record that the date range and schema of table was imported to
    # 
    # TODO: Don't import .META. table!
    #
    def table(table_name, start_time, end_time, destination, versions)
      table_descriptor = @hbase.table_descriptor(table_name)
      
      cmd = "#{@hadoop_home}/bin/hadoop jar #{@hbase_home}/hbase-#{@hbase_version}.jar import " +
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
