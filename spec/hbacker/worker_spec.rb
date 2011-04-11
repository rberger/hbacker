require File.join(File.dirname(__FILE__), "..", "spec_helper")

require "hbacker"
module Worker
end


describe Worker, "Stalker jobs" do
  # Just enough code to execute the worker job in the scope of the test
  module Stalker
    extend self
    @@handler = {}
    def log(str) 
    end
    def job(j, &block)
      @@handler ||= {}
      @@handler[j] = block
      @@handler
    end
    def error(&blk)
      @@error_handler = blk
    end

    def error_handler
      @@error_handler
    end

    def handler
      @@handler
    end
    def clear!
      @@handler = nil
      @@before_handlers = nil
      @@error_handler = nil
    end
  end
  
  before :all do
    Hbacker.log.level = Logger::ERROR
    require File.expand_path(File.join(File.dirname(__FILE__), "../../", "lib", "worker")) 

    @tstargs = {
      :table_name => "furtive_production_consumer_events_00b2330f-d66e-0e38-a6bf-0c2b529a36a2",
      :start_time => 1288537130080,
      :end_time => 1291233436567,
      :destination => "s3n://somebucket/#{@table_name}/",
      :versions => 100000,
      :session_name => "20110101_111111",
      :stargate_url => "http://example.com",
      :aws_access_key_id => 'aws_access_key_id',
      :aws_secret_access_key => 'aws_secret_access_key',
      :hbase_name => "hbase_master0",
      :hbase_host => 'hbase-master0-production.runa.com',
      :hbase_port => 8888,
      :hbase_home => "/mnt/hbase",
      :hadoop_home =>"/mnt/hadoop",
      :hbase_version => '0.20.3',
      :mapred_max_jobs => 10,
      :log_level => Hbacker.log.level,
      :reiteration_time => 5,
      :reset_instance_vars  => true
    }
  end

  before :each do
    # Can not do mocks in before :all
    @hbase_mock = mock('@hbase_mock')
    @table_descriptor = mock('@table_descriptor')
    @hbase_mock.stub(:wait_for_mapred_queue).and_return(:ok)
    @hbase_mock.stub(:table_descriptor).and_return(@table_descriptor)
    Hbacker::Hbase.stub(:new).and_return(@hbase_mock)
    @s3_mock = mock('@s3_mock')
    Hbacker::S3.stub(:new).and_return(@s3_mock)
    @db_mock = mock('@db_mock')
    @db_mock.stub(:column_descriptors)
    Hbacker::Db.stub(:new).and_return(@db_mock)
    @export_mock = mock('@export_mock')
    Hbacker::Export.stub(:new).and_return(@export_mock)
    @import_mock = mock('@import_mock')
    Hbacker::Import.stub(:new).and_return(@import_mock)
  end

  describe Worker, "Export Job" do
    it "should build a proper Hbacker::Export#table command when table not empty" do
      @hbase_mock.stub(:table_has_rows?).and_return(true)

      @export_mock.should_receive(:table).with(@tstargs[:table_name], @tstargs[:start_time], 
        @tstargs[:end_time], @tstargs[:destination], @tstargs[:versions], @tstargs[:session_name])

      Stalker.handler['queue_table_export'].call(@tstargs)
    end

    it "should build a proper Hbacker::Db#exported_table_info command when table is empty" do
      @hbase_mock.stub(:table_has_rows?).with(@tstargs[:table_name]).and_return(false)
    
      @db_mock.should_receive(:exported_table_info).with(@tstargs[:table_name], 
      @tstargs[:start_time], @tstargs[:end_time], @table_descriptor, @tstargs[:versions], 
        @tstargs[:session_name], true)

      Stalker.handler['queue_table_export'].call(@tstargs)
    end
  end

  describe Worker, "Import Job" do
    it "should build a proper Hbacker::Import#table command" do
      @hbase_mock.stub(:table_has_rows?).and_return(true)

      @import_mock.should_receive(:table).with(@tstargs[:table_name], @tstargs[:source], @tstargs[:import_session_name], @table_description)
      Stalker.handler['queue_table_import'].call(@tstargs)
    end
  end

  describe Worker, "Export Error Handling" do
    it "should call Db#exported_table_info with proper values on Hbase#wait_for_mapred_queue error" do
      # So we don't get the error logs
      Hbacker.log.level = Logger::UNKNOWN
      @tstargs[:log_level] = Logger::UNKNOWN
      
      @job_mock = mock("@job_mock")
      @hbase_mock.stub(:table_has_rows?).with(@tstargs[:table_name]).and_return(true)
      @hbase_mock.stub(:wait_for_mapred_queue).and_return(false)

      @job_mock.should_receive(:bury)
      @db_mock.should_receive(:exported_table_info).with(@tstargs[:table_name], @tstargs[:start_time], 
        @tstargs[:end_time], nil, @tstargs[:versions], @tstargs[:session_name], false, 
        hash_including(:info => "Worker::WorkerError: Export Timedout waiting 20000 seconds for Hadoop Map Reduce Queue to be less than 10 jobs"))
      begin
        Stalker.handler['queue_table_export'].call(@tstargs)
      rescue  => e
        Stalker.error_handler.call(e, 'queue_table_export', @tstargs, @job_mock)
      end
    end
  end

  describe Worker, "Import Error Handling" do
    it "should call Db#imported_table_info with proper values on Hbase#wait_for_mapred_queue error" do
      # So we don't get the error logs
      Hbacker.log.level = Logger::UNKNOWN
      @tstargs[:log_level] = Logger::UNKNOWN
      
      @job_mock = mock("@job_mock")
      @hbase_mock.stub(:table_has_rows?).with(@tstargs[:table_name]).and_return(true)
      @hbase_mock.stub(:wait_for_mapred_queue).and_return(false)

      @job_mock.should_receive(:bury)
      @db_mock.should_receive(:imported_table_info).with(@tstargs[:table_name], @tstargs[:session_name], false, 
        hash_including(:info => "Worker::WorkerError: Import Timedout waiting 20000 seconds for Hadoop Map Reduce Queue to be less than 10 jobs"))
      begin
        Stalker.handler['queue_table_import'].call(@tstargs)
      rescue  => e
        Stalker.error_handler.call(e, 'queue_table_import', @tstargs, @job_mock)
      end
    end
  end
end