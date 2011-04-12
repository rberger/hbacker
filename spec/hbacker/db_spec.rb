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

describe Hbacker::Db, "initialize" do
  before :each do
    @aws_access_key_id = "the_id"
    @aws_secret_access_key = "the_key"
    @hbase_name = "hbase_master0_production_runa_com"
    @db_export_class_names = %w(ExportSession ExportedHbaseTable ExportedColumnDescriptor)
    @db_import_class_names = %w(ImportSession ImportedHbaseTable ImportedColumnDescriptor)
    @connection = mock("@connection")
    RightAws::SdbInterface.stub(:new).and_return(@connection)
  end

  after :each do
    # Deletes the dynamically created classes
    (@db_export_class_names + @db_import_class_names).each do |name|
      sym = name.to_sym
      Object.instance_eval {remove_const sym} if Object.const_defined?(sym)
    end
  end
  
  it "should create the proper SimpleDB export classes for this app" do
    @connection.should_receive(:create_domain).once.with("export_info")
    @connection.should_receive(:create_domain).once.with("exported_#{@hbase_name}_tables")
    @connection.should_receive(:create_domain).once.with("exported_#{@hbase_name}_column_descriptors")
    db = Hbacker::Db.new(:export, @aws_access_key_id, @aws_secret_access_key, @hbase_name)
    @db_export_class_names.each do |name|
      Object.const_defined?(name).should be_true
    end
  end
  
  it "should create the proper SimpleDB import classes for this app" do
    @connection.should_receive(:create_domain).once.with("import_info")
    @connection.should_receive(:create_domain).once.with("imported_#{@hbase_name}_tables")
    @connection.should_receive(:create_domain).once.with("imported_#{@hbase_name}_column_descriptors")
    db = Hbacker::Db.new(:import, @aws_access_key_id, @aws_secret_access_key, @hbase_name)
    @db_import_class_names.each do |name|
      Object.const_defined?(name).should be_true
    end
  end

  it "export mode should have columns" do
    @connection.stub(:create_domain).with(any_args())
    db = Hbacker::Db.new(:export, @aws_access_key_id, @aws_secret_access_key, @hbase_name)
    @db_export_class_names.each do |name|
      Object.const_get(name).columns.class.should == RightAws::ActiveSdb::ColumnSet
    end
  end

  it "import mode should have columns" do
    @connection.stub(:create_domain).with(any_args())
    db = Hbacker::Db.new(:import, @aws_access_key_id, @aws_secret_access_key, @hbase_name)
    @db_import_class_names.each do |name|
      Object.const_get(name).columns.class.should == RightAws::ActiveSdb::ColumnSet
    end
  end
end

describe Hbacker::Db, "access methods" do
  before :each do
    @aws_access_key_id = "the_id"
    @aws_secret_access_key = "the_key"
    @hbase_name = "hbase_master0_production_runa_com"
    @db_export_class_names = %w(ExportSession ExportedHbaseTable ExportedColumnDescriptor)
    @db_import_class_names = %w(ImportSession ImportedHbaseTable ImportedColumnDescriptor)
    @connection = mock("@connection")
    @connection.stub(:create_domain).with(any_args())
    RightAws::SdbInterface.stub(:new).and_return(@connection)
    
    @session_name = "20110403_025437"
    @dest_root = "s3n://runa_hbacker_test"
    @specified_start = 0
    @specified_end = 1301799217000
    @started_at = Time.now.utc
  end
  describe Hbacker::Db, "start_info" do
  
    it "should call the correct class based on :export mode" do
      @db_import = Hbacker::Db.new(:import, @aws_access_key_id, @aws_secret_access_key, @hbase_name)
      # @db_export = Hbacker::Db.new(:export, @aws_access_key_id, @aws_secret_access_key, @hbase_name)
      ImportSession.should_receive(:create).once
      @db_import.start_info(@session_name, @dest_root, @specified_start, @specified_end, @started_at)
    end
    
    it "should call the correct class based on :import mode" do
      @db_export = Hbacker::Db.new(:export, @aws_access_key_id, @aws_secret_access_key, @hbase_name)
      ExportSession.should_receive(:create).once
      @db_export.start_info(@session_name, @dest_root, @specified_start, @specified_end, @started_at)
    end
  end
end
