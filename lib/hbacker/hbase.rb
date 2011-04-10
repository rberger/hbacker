module Hbacker
  require "stargate"

  class Hbase
    attr_reader :hbase_home, :hadoop_home, :star_gate, :url, :hbase_host, :hbase_port

    def initialize(hbase_home, hadoop_home, hbase_host, hbase_port=nil)
      @hbase_home = hbase_home
      @hadoop_home = hadoop_home
      @hbase_host = hbase_host
      @hbase_port = hbase_port
      @hbase_port_string = hbase_port.nil? ? "" : ":#{hbase_port}"
      @url = "http://#{@hbase_host}#{@hbase_port_string}"

      Hbacker.log.debug "@stargate = Stargate::Client.new(#{@url.inspect})"
      @stargate = Stargate::Client.new(@url)
    end

    class HbaseError < RuntimeError ; end

    ##
    # Get the Stargate::Model::TableDescriptor of the specified table from HBase
    def table_descriptor(table_name)
      @stargate.show_table(table_name)
    end

    ##
    # Get the list of the names of all HBase Tables in the cluster
    # @return [Array<String>] List of Table Names
    def list_names_of_all_tables
      tables = @stargate.list_tables
      tables.collect { |t| t.name}
    end

    ##
    # Detect if a table has any rows at least one row
    # Uses scanner to read one row.
    # @param [String] table_name Name of the table to check
    # @return [Boolean] True if there is a row, false if no rows
    #
    def table_has_rows?(table_name)
      scanner = @stargate.open_scanner(table_name)
      rows = @stargate.get_rows(scanner, limit = 1)
      not rows.empty?
    end

    ##
    # Create HBase Table
    # @param [String] name Name of the HBase Table to create
    # @param [Hash] schema Keypairs describing the Table Schema Hash
    # @return [Hash] status Status of create_table call
    # @option status [Stargate::Model::TableDescriptor] :created Everything is cool.
    # @option status [Stargate::Model::TableDescriptor] :exists If the table already exists. Value is the TableDescriptor
    # @option status [Stargate::TableFailCreateError] :hbase_table_create_error
    # @option status [Exception] :generic_exception Some other Exception
    # @option status [String] :wtf Should never get this exception
    #
    def create_table(name, schema)
      status = {:wtf => "Should  never be passed back"}
      result = nil

      begin
        result = @stargate.create_table(name, schema)
      rescue Stargate::TableExistsError => e
        Hbacker.log.warn "Hbacker::Hbase#create_table: Table #{name} already exists. Continuing"
        status = {:exists =>  @stargate.show_table(name)}
        return status
      rescue Stargate::TableFailCreateError => e
        status ={:hbase_table_create_error => e}
        return status
      rescue Exception => e
        status = {:generic_exception => e}
        return status
      end
      return {:created => result}
    end

    def jobs_in_hadop_queue
      cmd = "#{@hadoop_home}/bin/hadoop job -list"
      results = `#{cmd}`
      m = /^(\d+) jobs currently running/.match(results)
      jobs = m.nil? ? 0 : m[1].to_i
    end
    
    def wait_for_mapred_queue(threshold, count, delay)
      while count > 0
        jobs = jobs_in_hadop_queue
        Hbacker.log.debug "Hbacker::Hbase.wait_for_mapred_queue: #{jobs} in Hadoop Queue"
        return :ok if jobs <= threshold
        Hbacker.log.debug "Hbacker::Hbase.wait_for_mapred_queue: #{jobs} > #{threshold}. Waiting #{count * delay} seconds"
        sleep delay
        count -= 1
      end
      return :ok if (jobs = jobs_in_hadop_queue) <= threshold
      Hbacker.log.debug "Hbacker::Hbase.wait_for_mapred_queue: TIMEOUT after waiting #{count * delay} seconds. Final job count: #{jobs}"
      return :timeout
    end
  end
end

