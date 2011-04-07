module Hbacker
  require "hbacker"
  require "stalker"
  require File.expand_path(File.join(File.dirname(__FILE__), "../", "stalker"))  
  require 'timeout'

  ##
  # Will wait until the Stalker/Beanstalk  ready jobs goes above threshold
  # @param [Integer] threshold Function will wait until the number of ready jobs in the queue goes above this value
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
  def self.wait_for_hbacker_queue(queue_name, threshold, timeout)
    start = Time.now.utc
    stats = {}
    begin
      Timeout::timeout(timeout) do |timeout_length|
        bs = Stalker.beanstalk
        loop do
          stats = Hbacker.transform_keys_to_symbols(bs.stats_tube(queue_name))
          break if stats[:current_jobs_ready] < threshold
        end
      end
    rescue Timeout::Error
      stats[:duration] = Time.now.utc - start
      stats[:ok] = false
      stats[:timeout] = true
      Hbacker.log.warn "Hbacker.wait_for_hbacker_queue queue_name: #{queue_name.inspect}: Timeout"
      return stats
    rescue Beanstalk::NotFoundError
      Hbacker.log.debug "Hbacker.wait_for_hbacker_queue queue_name: #{queue_name.inspect}: No jobs Found"
    end
    stats[:duration] = (Time.now.utc - start)
    stats[:ok] = true
    Hbacker.log.debug "Hbacker.wait_for_hbacker_queue: #{queue_name.inspect}: OK ready: #{stats[:current_jobs_ready]} reserved: #{stats[:current_jobs_reserved]} waiting: #{stats[:current_waiting]}"
    return stats
  end
  
  #take keys of hash and transform those to a symbols
  def self.transform_keys_to_symbols(input)
    return input if not input.is_a?(Hash)
    hsh = input.inject({}) do |memo,(k,v)|
      k = k.class == Symbol ? k : k.downcase.gsub(/\s+|-/, "_").to_sym
      memo[k] = self.transform_keys_to_symbols(v); memo
    end
    return hsh
  end
  
end