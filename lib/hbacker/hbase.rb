module Hbacker
  require "stargate"
  
  class Hbase
    attr_reader :hbase_home, :hadoop_home, :star_gate, :url
    
    def initialize(hbase_home, hadoop_home, master_host, master_port=nil)
      @hbase_home = hbase_home
      @hadoop_home = hadoop_home
      @master_host = master_host
      @master_port = master_port
      @master_port_string = master_port.nil? ? "" : ":#{master_port}"
      @url = "http://#{@master_host}#{@master_port_string}"

      Hbacker.log.debug "@stargate = Stargate::Client.new(#{@url.inspect})"
      @stargate = Stargate::Client.new(@url)
    end
    
    ##
    # Get the Stargate::Model::TableDescriptor of the specified table from HBase
    def table_descriptor(table_name)
      @stargate.show_table(table_name)
    end
    
    ##
    # Get the list of HBase Tables in the cluster
    # @returns [Array<Stargate::Model::TableDescriptor>] List of TableDescriptors
    def list_tables
      @stargate.list_tables
    end
    
    ##
    # Create HBase Table
    # @param [String] name Name of the HBase Table to create
    # @param [Hash] schema Keypairs describing the Table Schema Hash
    # @return [Hash] status Status of create_table call
    # @option status [Stargate::Model::TableDescriptor] :created Everything is cool.
    # @option status [Stargate::Model::TableDescriptor] :exists If the table already exists. Value is the TableDescriptor
    # @option status [Stargate::TableFailCreateError] :hbase_table_create_error
    # @optioin status [Exception] :generic_exception Some other Exception
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
  end
end

