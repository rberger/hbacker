require File.join(File.dirname(__FILE__), "..", "spec_helper")

require "hbacker"
require 'pp'

describe Hbacker, "wait_for_hbacker_queue" do
  before :all do
    Hbacker.log.level = Logger::ERROR
  end
  
  before :each do
    results = `pgrep beanstalkd`
    raise "Already a beanstalkd running: PID: #{results}" if $?.exitstatus == 0

    results = `beanstalkd -d`
    raise "Failed to start beanstalkd: #{results}" if $?.exitstatus > 0
    
    @queue_name = "wait_for_hbacker_queue_test"
    # Stalker.enqueue(@queue_name)
  end

  after :each do
    pid = `pkill beanstalkd`
    raise "Failed to kill beanstalkd: #{pid}" if $?.exitstatus > 0
  end
  
  it "should return with ok, duration values, but no others if there are no jobs" do
    results = Hbacker.wait_for_hbacker_queue(@queue_name, 10, 10)
    results[:ok].should be_true
    results[:duration].should > 0
    results[:current_jobs_ready].should be_nil
  end
  
  it "should return with ok status if there are less jobs than the threshold" do
    Stalker.enqueue(@queue_name)
    results = Hbacker.wait_for_hbacker_queue(@queue_name, 2, 10)
    results[:ok].should be_true
    results[:duration].should > 0
    results[:current_jobs_ready].should == 1
  end

  it "should return with timeout status if there are more jobs than the threshold and timeout passes" do
    Stalker.enqueue(@queue_name)
    Stalker.enqueue(@queue_name)
    Stalker.enqueue(@queue_name)
    results = Hbacker.wait_for_hbacker_queue(@queue_name, 1, 1)
    results[:ok].should_not be_true
    results[:timeout].should be_true
    results[:duration].should > 0
    results[:current_jobs_ready].should > 1
  end

end

describe Hbacker, "transform_keys_to_symbols" do
  it "should transform string keys to symbols" do
    h = {
      'foo'  => 1,
      "bar" => 'hello'
    }
    transformed_hash = Hbacker.transform_keys_to_symbols(h)
    transformed_hash.keys.each do |k|
      k.class.should == Symbol
    end
  end
end
