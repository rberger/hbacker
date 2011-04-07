require File.join(File.dirname(__FILE__), "..", "spec_helper")

require "hbacker"

describe Hbacker, "queue_table_export Stalker job" do
  # Just enough code to execute the worker job in the scope of the test
  module Stalker
    extend self
    @@handler = {}
    def log(str) 
    end
    def job(j, &block)
      # STDERR.puts "JOB Top: @@handler: #{@@handler.inspect}"
      @@handler = {}
      @@handler[j] = block
      @@handler
    end
    def error(&blk)
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
    @worker_file = File.expand_path(File.join(File.dirname(__FILE__), "../../", "lib", "worker.rb"))
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
     Hbacker::Db.stub(:new).and_return(@db_mock)
     @export_mock = mock('@export_mock')
     Hbacker::Export.stub(:new).and_return(@export_mock)
  end

  it "should build a proper Hbacker::Export#table command when table not empty" do
    @hbase_mock.stub(:table_has_rows?).and_return(true)

    @export_mock.should_receive(:table).with(@tstargs[:table_name], @tstargs[:start_time], 
      @tstargs[:end_time], @tstargs[:destination], @tstargs[:versions], @tstargs[:session_name])
    Stalker.handler['queue_table_export'].call(@tstargs)
  end

  it "should build a proper Hbacker::Db#table_info command when table is empty" do
    @hbase_mock.stub(:table_has_rows?).with(@tstargs[:table_name]).and_return(false)
    
    @db_mock.should_receive(:table_info).with(:export, @tstargs[:table_name], 
    @tstargs[:start_time], @tstargs[:end_time], @table_descriptor, @tstargs[:versions], 
      @tstargs[:session_name], true, false)

    Stalker.handler['queue_table_export'].call(@tstargs)
  end
end