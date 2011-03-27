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
    # @option opts [String] :source_root Scheme://root_path of the Source directory of backups
    # @option opts [String] :backup_name Name of the backup session / subdirectory containing table directories
    # 
    def specified_tables(opts)
      table_names = @db.table_names_by_backup_name(opts[:backup_name], opts[:source_root])
      table_names.each do |table|
        source = "#{opts[:source_root]}#{opts[:backup_name]}/#{table}/"
        Hbacker.log.info "Backing up #{table} to #{source}"
        Hbacker.log.debug "self.table(#{table}, #{source})"
        self.table(table, source)
      end
    end
    
    ##
    # Uses Hadoop to import specfied table from source file system to target HBase Cluster
    # @param [String] table_name The name of the table to import
    # @param [String] source scheme://source_path/backup_name/ to the backup data
    #
    def table(table_name, source)
      
      table_status = @hbase.create_table(table_name, table_description)
      
      cmd = "#{@hadoop_home}/bin/hadoop jar #{@hbase_home}/hbase-#{@hbase_version}.jar import " +
        "#{table_name} #{source}"
      Hbacker.log.debug "About to execute #{cmd}"
      cmd_output = `#{cmd} 2>&1`
      # Hbacker.log.debug "cmd output: #{cmd_output}"
      
      Hbacker.log.debug "$?.exitstatus: #{$?.exitstatus.inspect}"
      
      if $?.exitstatus > 0
        Hbacker.log.error"Hadoop command failed: #{cmd}"
        Hbacker.log.error cmd_output
        exit(-1)
      end
    end
  end
end
