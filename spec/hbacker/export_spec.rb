require File.join(File.dirname(__FILE__), "..", "spec_helper")

require "hbacker"
require "hbacker/db"
require "hbacker/export"

describe Hbacker::Export do
  before :each do
    @table_name = "furtive_production_consumer_events_00b2330f-d66e-0e38-a6bf-0c2b529a36a2"
    @start_time = 1288537130080
    @end_time = 1291233436567
    @versions = 100000
    @session_name = 20110101_111111
    @dest_root = "s3n://somebucket/"
    @destination = "#{@dest_root}#{@session_name}/#{@table_name}/"
  
    @hbase_mock = mock('@hbase_mock')
    @hbase_mock.stub(:table_descriptor).with(@table_name)
    @db_mock = mock('@db_mock')
    @db_mock.stub(:exported_table_info)
    @db_mock.stub(:start_info)
    @db_mock.stub(:end_info)
    Hbacker::Db.stub(:new).and_return(@db_mock)
    @s3_mock = mock('@s3_mock')
    @s3_mock.stub(:save_info)
    Hbacker::S3.stub(:new).and_return(@s3_mock)
    @hadoop_hm =  "/mnt/hadoop"
    @hbase_hm = "/mnt/hbase"
    @hbase_vsn = "0.20.3"
    @reiteration_time = 15
    @mapred_max_jobs = 10
  end

  describe Hbacker::Export, "specified_tables" do
    before :each do
      Hbacker.stub(:wait_for_hbacker_queue).and_return({:ok => true})
    end
    
    it "should call Export#queue_table_export_job with proper parameters" do
      timeout = 1000
      opts = {
        :session_name => @session_name,
        :dest_root => @dest_root,
        :start_time => @start_time,
        :end_time => @end_time,
        :tables => [@table_name],
        :workers_watermark => 10,
        :workers_timeout => timeout,
        :versions => @versions,
        :timeout => timeout,
        :reiteration_time => @reiteration_time,
        :mapred_max_jobs => @mapred_max_jobs
      }
      
      export = Hbacker::Export.new(@hbase_mock, @db_mock, @hbase_hm, @hbase_vsn, @hadoop_hm, @s3_mock)
      export.should_receive(:queue_table_export_job).with(@table_name, @start_time, @end_time, 
        @destination, @versions, @session_name, timeout, @reiteration_time, @mapred_max_jobs)
      export.specified_tables(opts)
    end
  end

  describe Hbacker::Export, "table" do
  
    it "should shell out the correct hbase command" do
      export = Hbacker::Export.new(@hbase_mock, @db_mock, @hbase_hm, @hbase_vsn, @hadoop_hm, @s3_mock)
    
      export.should_receive(:`).with("#{@hadoop_hm}/bin/hadoop jar #{@hbase_hm}/hbase-#{@hbase_vsn}.jar export " +
        "#{@table_name} #{@destination} #{@versions} #{@start_time} #{@end_time} 2>&1").and_return("hadoop stdout stream")
    
      export.table(@table_name, @start_time, @end_time, @destination, @versions, @session_name)
    end
  end
end