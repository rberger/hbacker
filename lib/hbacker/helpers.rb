module Hbacker
  require "hbacker"
  require "stalker"

  ##
  # Will wait until the Stalker/Beanstalk queue of waiting jobs goes below threshold
  # @param [Integer] threshold Function will wait until the number of jobs in the queue goes below this value
  # @param [Integer] timeout Function will return after timeout seconds with a :timeout status
  # @return [Hash] return_value
  # @option return_value [Boolean] :timeout Only set if there was a timeout
  # @option return_value [Boolean] :ok Only set if number of jobs went below the threshold before timeout
  # @option return_value [Hash] :stats The 
  def wait_for_hbacker_queue(threshold, timeout)
    
  end
  
end