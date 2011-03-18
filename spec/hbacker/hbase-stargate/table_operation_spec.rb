require File.join(File.dirname(__FILE__), "..", "..", "spec_helper")
require 'hbacker/stargate'
require 'stargate'

describe Stargate::Model, "#column_families_to_hash" do
  before :all do
    @column_families = []
    @column_descriptor_0 = Stargate::Model::ColumnDescriptor.new(:name => 'hobbit',
      :max_version => 100000,
      :compression => Stargate::Model::CompressionType::NONE,
      :in_memory => true,
      :block_cache => false,
      :ttl => -1,
      :max_cell_size => 2147483647,
      :bloomfilter => false)

    @column_families <<  @column_descriptor_0

    @column_descriptor_1 = Stargate::Model::ColumnDescriptor.new(:name => 'sauron',
      :max_version => 3,
      :compression => Stargate::Model::CompressionType::NONE,
      :in_memory => false,
      :block_cache => true,
      :ttl => -1,
      :max_cell_size => 2147483647,
      :bloomfilter => false)
      
    @column_families <<  @column_descriptor_1
    
    @table_descriptor = Stargate::Model::TableDescriptor.new(
      :name => "test", 
      :column_families => @column_families)
  end
    
  it "should have two column_families" do
    @table_descriptor.column_families_to_hash.count.should == 2
  end
  
  it "should be accessible as an array of hashes" do
    @table_descriptor.column_families_to_hash.first[:name].should == 'hobbit'
    @table_descriptor.column_families_to_hash[1][:name].should == 'sauron'
  end
end

describe Stargate::Operation::TableOperation, "#create_table_from_table_descriptor" do
  before :each do
    @connection = mock('@connection')
    Net::HTTP.stub(:new).and_return(@connection)
    url = ENV["STARGATE_URL"].nil? ? "http://localhost:8080" : ENV["STARGATE_URL"]
    @client = Stargate::Client.new(url)
    @client.stub(:create_table)

    @column_descriptor_1 = Stargate::Model::ColumnDescriptor.new(:name => 'hobbit',
    :max_version => 3,
    :compression => Stargate::Model::CompressionType::NONE,
    :in_memory => false,
    :block_cache => false,
    :ttl => -1,
    :max_cell_size => 2147483647,
    :bloomfilter => false)
    @column_families = []
    @column_families <<  @column_descriptor_1

    @table_descriptor = Stargate::Model::TableDescriptor.new(:name => "test", :column_families => @column_families)
  end

  it "should accept a single argument of type Stargate::Model::TableDescriptor" do
    result = @client.create_table_from_table_descriptor(@table_descriptor)
  end
  
  it "should accept two arguments of type Stargate::Model::TableDescriptor and string" do
    result = @client.create_table_from_table_descriptor(@table_descriptor, 'tolken')
  end
  
  it "should generate an error with invalid arguments" do
    expect { @client.create_table_from_table_descriptor('tolken') }.to raise_error
  end
  
  it "should pass a name and hash to Stargate::Operation::TableOperation#create_table" do
    @client.should_receive(:create_table).with(instance_of(String), hash_including(:name => 'hobbit'))
    result = @client.create_table_from_table_descriptor(@table_descriptor, 'tolken')
    
  end
end
