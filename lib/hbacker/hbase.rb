module Hbacker
  require "stargate"
  
  class HBase
    attr_reader :hbase_home, :hadoop_home, :star_gate
    
    def initialize(master_host, master_port, hbase_home, hadoop_home)
      @hbase_home = hbase_home
      @hadoop_home = hadoop_home
      @stargate = Stargate::Client.new("http://master_host:master_port")
    end
    
    def 
    def 
  end
end

