module Hbacker
  class Export
    require "hbacker"
    ##
    # Initialize the Export Instance
    #
    def initialize(hbase, db, hadoop_hom, hbase_homee, hbase_version)
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
    def all_tables(options)
      
    end
    
    ##
    # Iterates thru the list of tables calling Export#table to do the Export to the specified dest
    def specified_tables(options)
    end
    
    ##
    # Queries HBase for the table's schema
    # 
    def table(table, start_time, end_time, versions, destination)
      cmd = "#{@hadoop_home}/bin/hadoop jar #{hbase_home}/hbase-#{@hbase_version}.jar export " +
        "#{table} #{destination} #{versions} #{start_time} #{end_time}"
      STDERR.puts "About to execute #{cmd}"
      cmd_output = %x[#{cmd} 2>&1]
      STDERR.puts "cmd output: #{cmd_output}"
      
      STDERR.puts "$?.exitstatus: #{$?.exitstatus.inspect}"
      
      if $?.exitstatus > 0
        STDERR.puts"Hadoop command failed:"
        STDERR.puts cmd_output
        exit(-1)
      end
      schema = @hbase.schema(table)
      @db.record_table_info(table, start_time, end_time, versions, schema)
    end
  end
end
