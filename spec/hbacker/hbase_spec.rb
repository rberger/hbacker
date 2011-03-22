require File.join(File.dirname(__FILE__), "..", "spec_helper")

require "hbacker"
require "hbacker/hbase"
require "stargate"

describe Hbacker::Hbase, "initialize" do
  before :each do
    @host = 'hbase-master0-production.runa.com'
    @port = 8808
    @hbase_hm = "/mnt/hbase"
    @hadoop_hm = "/mnt/hadoop"
    @stargate = mock('@stargate')
    Stargate::Client.new.stub(:new).and_return(@stargate)
  
  end
  
  it "should create an instance of Stargate::Client from the correct url with no port specified" do
    Stargate::Client.should_receive(:new).with('http://hbase-master0-production.runa.com').and_return(@stargate)
    Hbacker::Hbase.new(@hbase_hm, @hadoop_hm, @host)
  end
end
