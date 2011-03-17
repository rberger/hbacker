require File.join(File.dirname(__FILE__), "..", "..", "spec_helper")
require 'hbacker/stargate'
require 'stargate'

describe Stargate::Operation::TableOperation, "#create_table_from_table_descriptor" do
  before :each do
    @connection = mock('@connection')
    Net::HTTP.stub(:new).and_return(@connection)
    url = ENV["STARGATE_URL"].nil? ? "http://localhost:8080" : ENV["STARGATE_URL"]
    @client = Stargate::Client.new(url)
    @client.stub(:create_table)
    
    @column_descriptor_1 = Stargate::Model::ColumnDescriptor.new(:name => 'habbit',
                                   :max_version => 3,
                                   :compression => Stargate::Model::CompressionType::NONE,
                                   :in_memory => false,
                                   :block_cache => false,
                                   :ttl => -1,
                                   :max_cell_size => 2147483647,
                                   :bloomfilter => false)
    @column_families = []
    @column_families <<  @column_descriptor_1
  end

  it "should accept a single argument of type Stargate::Model::TableDescriptor" do
    table_descriptor = Stargate::Model::TableDescriptor.new(:name => "test", :column_families => @column_families)
    result = @client.create_table_from_table_descriptor(table_descriptor)
  end
end
