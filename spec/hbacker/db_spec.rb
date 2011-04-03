require File.join(File.dirname(__FILE__), "..", "spec_helper")

require "hbacker"

describe Hbacker::Db, "initialize" do
  before :each do
    @aws_access_key_id = "the_id"
    @aws_secret_access_key = "the_key"
    @hbase_name = "hbase_master0_production_runa_com"
    @db_class_names = %w(ExportSession ExportedHbaseTable ExportedColumnDescriptor
      ImportSession ImportedHbaseTable ImportedColumnDescriptor)
    @connection = mock("@connection")
    RightAws::SdbInterface.stub(:new).and_return(@connection)
  end

  after :each do
    # Deletes the dynamically created classes
    @db_class_names.each do |name|
      sym = name.to_sym
      Object.instance_eval {remove_const sym} if Object.const_defined?(sym)
    end
  end
  
  it "should create the proper SimpleDB classes for this app" do
    @connection.should_receive(:create_domain).once.with("export_info")
    @connection.should_receive(:create_domain).once.with("exported_#{@hbase_name}_tables")
    @connection.should_receive(:create_domain).once.with("exported_#{@hbase_name}_column_descriptors")
    @connection.should_receive(:create_domain).once.with("import_info")
    @connection.should_receive(:create_domain).once.with("imported_#{@hbase_name}_tables")
    @connection.should_receive(:create_domain).once.with("imported_#{@hbase_name}_column_descriptors")
    db = Hbacker::Db.new(@aws_access_key_id, @aws_secret_access_key, @hbase_name)
    @db_class_names.each do |name|
      Object.const_defined?(name).should be_true
    end
  end
  
  it "should have columns" do
    @connection.stub(:create_domain).with(any_args())
    db = Hbacker::Db.new(@aws_access_key_id, @aws_secret_access_key, @hbase_name)
    @db_class_names.each do |name|
      Object.const_get(name).columns.class.should == RightAws::ActiveSdb::ColumnSet
    end
  end
end

describe Hbacker::Db, "access methods" do
  before :each do
    @aws_access_key_id = "the_id"
    @aws_secret_access_key = "the_key"
    @hbase_name = "hbase_master0_production_runa_com"
    @db_class_names = %w(ExportSession ExportedHbaseTable ExportedColumnDescriptor
      ImportSession ImportedHbaseTable ImportedColumnDescriptor)
    @connection = mock("@connection")
    @connection.stub(:create_domain).with(any_args())
    RightAws::SdbInterface.stub(:new).and_return(@connection)
    @db = Hbacker::Db.new(@aws_access_key_id, @aws_secret_access_key, @hbase_name)
    
    @session_name = "20110403_025437"
    @dest_root = "s3n://runa_hbacker_test"
    @specified_start = 0
    @specified_end = 1301799217000
    @started_at = Time.now.utc
  end
  describe Hbacker::Db, "start_info" do
  
    it "should call the correct class based on mode" do
      ExportSession.should_receive(:create).once
      @db.start_info(:export, @session_name, @dest_root, @specified_start, @specified_end, @started_at)
      ImportSession.should_receive(:create).once
      @db.start_info(:import, @session_name, @dest_root, @specified_start, @specified_end, @started_at)
    
      lambda do 
        @db.start_info(:foo, @session_name, @dest_root, @specified_start, @specified_end, 
          @started_at) 
      end.should raise_error(NoMethodError)
    
    end
  end
end
