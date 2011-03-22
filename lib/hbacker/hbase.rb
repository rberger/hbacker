module Hbacker
  require "stargate"
  
  class Hbase
    attr_reader :hbase_home, :hadoop_home, :star_gate
    
    def initialize(hbase_home, hadoop_home, master_host, master_port=nil)
      @hbase_home = hbase_home
      @hadoop_home = hadoop_home
      @master_host = master_host
      @master_port = master_port
      @master_port_string = master_port.nil? ? "" : ":#{master_port}" 

      Hbacker.log.debug " @stargate = Stargate::Client.new(\"http://#{@master_host}#{@master_port_string}\")"
      @stargate = Stargate::Client.new("http://#{@master_host}#{@master_port_string}")
    end
    
    ##
    # Get the Stargate::Model::TableDescriptor of the specified table from HBase
    def table_descriptor(table_name)
      @stargate.show_table(table_name)
    end
    
    def list_tables
      @stargate.list_tables
    end
  end
end

