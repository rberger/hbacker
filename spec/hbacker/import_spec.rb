require File.join(File.dirname(__FILE__), "..", "spec_helper")

require "hbacker"
require "hbacker/db"
require "hbacker/import"

describe Hbacker::Import do
  before :each do
    @table_name = "furtive_production_consumer_events_00b2330f-d66e-0e38-a6bf-0c2b529a36a2"
    @start_time = 1288537130080
    @end_time = 1291233436567
    @versions = 100000
    @session_name = 20110101_111111
    @import_session_name = 20110201_222222
    @source_root = "s3n://somebucket/"
    @source = "#{@source_root}#{@session_name}/#{@table_name}/"
  
    @hbase_mock = mock('@hbase_mock')
    @hbase_mock.stub(:table_descriptor).with(@table_name)
    @hbase_mock.stub(:create_table).and_return(true)
    @db_mock = mock('@db_mock')
    @db_mock.stub(:imported_table_info)
    @db_mock.stub(:start_info)
    @db_mock.stub(:end_info)
    @db_mock.stub(:table_names).and_return([@table_name, 'bar'])
    Hbacker::Db.stub(:new).and_return(@db_mock)
    @s3_mock = mock('@s3_mock')
    @s3_mock.stub(:save_info)
    Hbacker::S3.stub(:new).and_return(@s3_mock)
    @hadoop_hm =  "/mnt/hadoop"
    @hbase_hm = "/mnt/hbase"
    @hbase_vsn = "0.20.3"
    @reiteration_time = 15
    @mapred_max_jobs = 10
    @restore_empty_tables = false
  end

  describe Hbacker::Import, "specified_tables" do
    before :each do
      Hbacker.stub(:wait_for_hbacker_queue).and_return({:ok => true})
    end
    
    it "should call Import#queue_table_import_job with proper parameters" do
      timeout = 1000
      opts = {
        :session_name => @session_name,
        :source_root => @source_root,
        :tables => [@table_name],
        :workers_watermark => 10,
        :workers_timeout => timeout,
        :import_session_name => @import_session_name,
        :timeout => timeout,
        :reiteration_time => @reiteration_time,
        :mapred_max_jobs => @mapred_max_jobs
      }
      db_export = Hbacker::Db.new
      db_import = Hbacker::Db.new
      import = Hbacker::Import.new(@hbase_mock, db_export, db_import, @hbase_hm, @hbase_vsn, @hadoop_hm, @s3_mock)
      import.should_receive(:queue_table_import_job).with(@table_name, @source, 
        @session_name, @import_session_name, timeout, @reiteration_time, @mapred_max_jobs, @restore_empty_table)
      import.specified_tables(opts)
    end
  end

  describe Hbacker::Import, "table" do
  
    it "should shell out the correct hbase command" do
      db_export = Hbacker::Db.new
      db_import = Hbacker::Db.new
      import = Hbacker::Import.new(@hbase_mock, db_export, db_import, @hbase_hm, @hbase_vsn, @hadoop_hm, @s3_mock)
    
      import.should_receive(:`).with("#{@hadoop_hm}/bin/hadoop jar #{@hbase_hm}/hbase-#{@hbase_vsn}.jar import " +
        "#{@table_name} #{@source} 2>&1").and_return("hadoop stdout stream")
    
      import.table(@table_name, @source, @import_session_name, @table_description)
    end
  end
end