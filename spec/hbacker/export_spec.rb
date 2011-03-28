require File.join(File.dirname(__FILE__), "..", "spec_helper")

require "hbacker"
require "hbacker/db"
require "hbacker/export"

describe Hbacker::Export, "table" do
  before :each do
    @table_name = "furtive_production_consumer_events_00b2330f-d66e-0e38-a6bf-0c2b529a36a2"
    @start_time = 1288537130080
    @end_time = 1291233436567
    @destination = "s3n://somebucket/#{@table_name}/"
    @versions = 100000
    @backup_name = 20110101_111111
    
    @hbase_mock = mock('@hbase_mock')
    @hbase_mock.stub(:table_descriptor).with(@table_name)
    @db_mock = mock('@db_mock')
    @db_mock.stub(:record_table_info)
    Hbacker::Db.stub(:new).and_return(@db_mock)
    @s3_mock = mock('@s3_mock')
    @s3_mock.stub(:save_info)
    Hbacker::S3.stub(:new).and_return(@s3_mock)
    @hadoop_hm =  "/mnt/hadoop"
    @hbase_hm = "/mnt/hbase"
    @hbase_vsn = "0.20.3"
  end
  
  it "should shell out the correct hbase command" do
    export = Hbacker::Export.new(@hbase_mock, @db_mock, @hbase_hm, @hbase_vsn, @hadoop_hm, @s3_mock)
    
    export.should_receive(:`).with("#{@hadoop_hm}/bin/hadoop jar #{@hbase_hm}/hbase-#{@hbase_vsn}.jar export " +
      "#{@table_name} #{@destination} #{@versions} #{@start_time} #{@end_time} 2>&1").and_return("hadoop stdout stream")
    
    export.table(@table_name, @start_time, @end_time, @destination, @versions, @backup_name)
  end
end