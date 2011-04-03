require File.join(File.dirname(__FILE__), "..", "spec_helper")

require "hbacker"

describe Hbacker, "wait_for_hbacker_queue" do
  before :each do
    results = `pgrep beanstalkd`
    raise "Already a beanstalkd running: PID: #{results}" if $?.exitstatus == 0

    results = `beanstalkd -d`
    raise "Failed to start beanstalkd: #{results}" if $?.exitstatus > 0
    
    @queue_name = "wait_for_hbacker_queue_test"
    Stalker.enqueue(@queue_name)
  end

  after :each do
    pid = `pkill beanstalkd`
    raise "Failed to kill beanstalkd: #{pid}" if $?.exitstatus > 0
  end
  
  it "should return with ok status if there are no jobs" do
    Hbacker.wait_for_hbacker_queue(@queue_name, 10, 10)
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
