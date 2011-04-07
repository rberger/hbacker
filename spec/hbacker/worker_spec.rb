require File.join(File.dirname(__FILE__), "..", "spec_helper")

require "hbacker"

describe Hbacker, "queue_table_export Stalker job" do
  before :all do
    Hbacker.log.level = Logger::ERROR

    # Just enough code to execute the worker job in the scope of the test
    module Stalker
      extend self
      def log(str) 
      end
      def job(j, &block)
        @@handler = {}
        @@handler[j] = block
      end
      def error(&blk)
      end
      def handler
        @@handler
      end
      def clear!
        @@handler = nil
        @@handlers = nil
        @@before_handlers = nil
        @@error_handler = nil
      end
    end

    @args = {
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
      :reiteration_time => 5
    }
    @worker_file = File.expand_path(File.join(File.dirname(__FILE__), "../../", "lib", "worker.rb"))
  end

  before :each do
    Stalker.clear!
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

  # it "should build a proper Hbacker::Export#table command when table not empty" do
  #   puts "---- Spec 1 before stub @hbase_mock: #{@hbase_mock.inspect}"
  #   @hbase_mock.stub(:table_has_rows?).and_return(true)
  #   puts "Spec 1 after stub @hbase_mock: #{@hbase_mock.inspect}"
  # 
  #   @export_mock.should_receive(:table).with(@args[:table_name], @args[:start_time], 
  #   @args[:end_time], @args[:destination], @args[:versions], @args[:session_name])
  # 
  #   # This require evaluates the worker job using the module Stalker definition of job
  # 
  #   require File.expand_path(@worker_file)  
  #   Stalker.handler['queue_table_export'].call(@args)
  # end

  it "should build a proper Hbacker::Db#table_info command when table is empty" do
    puts "@worker_file: #{@worker_file.inspect}"
    puts "$\": #{$".assoc(@worker_file).inspect}"
    $".delete(@worker_file)
    @hbase_wrk = nil
    puts "--- Spec 2 before stub @hbase_mock: #{@hbase_mock.inspect}"
    @hbase_mock.stub(:table_has_rows?).with(@args[:table_name]).and_return(false)
    puts "Spec 2 after stub @hbase_mock: #{@hbase_mock.inspect}"

    @db_mock.should_receive(:table_info).with(:export, @args[:table_name], 
    @args[:start_time], @args[:end_time], @table_descriptor, @args[:versions], 
    @args[:session_name], true, false)
    puts "@args[:table_name]: #{@args[:table_name].inspect}"
    # This require evaluates the worker job using the module Stalker definition of job
    require File.expand_path(@worker_file)  
    Stalker.handler['queue_table_export'].call(@args)
  end
end