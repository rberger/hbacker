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
      puts "hbase_home: #{hbase_home.inspect} hadoop_home: #{hadoop_home.inspect} master_host: #{master_host.inspect} master_port: #{master_port.inspect}"
      puts "http://#{@master_host}#{@master_port_string}"
      @stargate = Stargate::Client.new("http://#{@master_host}#{@master_port_string}")
      puts "@stargate: #{@stargate.inspect}"
    end
  end
end

