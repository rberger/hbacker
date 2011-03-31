module Hbacker
  require "hbacker"
  require "stalker"
  require 'timeout'

  ##
  # Will wait until the Stalker/Beanstalk queue of waiting jobs goes below threshold
  # @param [Integer] threshold Function will wait until the number of jobs in the queue goes below this value
  # @param [Integer] timeout Function will return after timeout seconds with a :timeout status
  # @return [Hash] return_value
  # @option return_value [Boolean] :timeout Only set if there was a timeout
  # @option return_value [Boolean] :ok Only set if number of jobs went below the threshold before timeout
  # @option retunr_value [Float] :duration How long the function waited in secondes
  # @option return_value [Integer] :active_jobs Jobs Ready + Jobs Reserved
  # @option retunr_value [Integer] :current_jobs_ready
  # @option retunr_value [Integer] :current_jobs_reserved
  # @option retunr_value [Integer] :cmd-pause-tube
  # @option retunr_value [Integer] :current-jobs-buried
  # @option retunr_value [Integer] :current-jobs-delayed
  # @option retunr_value [Integer] :current-jobs-urgent
  # @option retunr_value [Integer] :current-using
  # @option retunr_value [Integer] :current-waiting
  # @option retunr_value [Integer] :current-watching
  # @option retunr_value [Integer] :pause
  # @option retunr_value [Integer] :pause-time-left
  # @option retunr_value [Integer] :total-jobs
  #
  def wait_for_hbacker_queue(queue_name, threshold, timeout)
    start = Time.now.utc
    stats = {}
    begin
      Timeout::timeout(timeout) do |timeout_length|
        bs = Stalker.beanstalk
        loop do
          stats = bs.stats_tube queue_name.transform_keys_to_symbols
          stats[:active_jobs] = stats[:current_jobs_ready] + stats[:current_jobs_reserved]
          break if stats[:active_jobs] < threshold
        end
      end
    rescue Timeout::Error
      stats[:duration] = Time.now.utc - start
      stats[:ok] = false
      stats[:timeout] = true
      return stats
    end
    stats[:duration] = (Time.now.utc - start)
    stats[:ok] = true
    return stats
  end
end