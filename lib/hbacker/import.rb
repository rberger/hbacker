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
    # Iterates thru the list of tables calling Import#table to do the Import to the specified dest
    # * Get the list of names based on the options from the Source directory 
    # * Create the table on the target HBase using the schema from Db
    # * Call the Hadoop process to move the file
    # @param [Hash] opts Hash from the CLI with all the options set
    #
    def specified_tables(opts)
      opts[:tables].each do |table|
        dest = "#{opts[:source_root]}#{opts[:backup_timestamp]}/#{table}/"
        Hbacker.log.info "Backing up #{table} to #{dest}"
        Hbacker.log.debug "self.table(#{table},#{ opts[:start]}, #{opts[:end]}, #{dest}, #{opts[:versions]})"
        self.table(table, opts[:start], opts[:end], dest, opts[:versions])
      end
    end
    
    ##
    # Uses Hadoop to import specfied table from source file system to target HBase Cluster
    # 
    # TODO: Don't import .META. or .ROOT. tables!
    #
    def table(table_name, start_time, end_time, source)
      
      table_descriptor = @hbase.table_descriptor(table_name)
      
      cmd = "#{@hadoop_home}/bin/hadoop jar #{@hbase_home}/hbase-#{@hbase_version}.jar import " +
        "#{table_name} #{source} #{versions} #{start_time} #{end_time}"
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
    
    ##
    # Get the list of table names from the 
  end
end
