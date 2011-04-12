# Copyright 2011 Robert J. Berger & Runa, Inc.
# 
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
# 
#      http://www.apache.org/licenses/LICENSE-2.0
# 
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.
#    
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
    @stargate.stub(:cluster_version)
    Stargate::Client.stub(:new).and_return(@stargate)
  end
  
  it "should create an instance of Stargate::Client from the correct url with no port specified" do
    Stargate::Client.should_receive(:new).with('http://hbase-master0-production.runa.com').and_return(@stargate)
    Hbacker::Hbase.new(@hbase_hm, @hadoop_hm, @host)
  end
end
