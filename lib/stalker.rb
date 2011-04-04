##
# Modified from Stalker 0.8.0 
# Addes functionality to pass in the Beanstalk::Job instance to the Stalker.job Proc
# Allows the Stalker.job Proc to access the Beanstalk::Job instance at the runtime of the Stalker.job Proc
# The Stalker.job Proc can then get stats or touch the Beanstalk::Job instance (i.e. reset its timeout counter)
#
module Stalker
  extend self

  # R. Berger added beanstalk_style_job param
  # @param [String] job Name of the Stalker.job
  # @param [Hash] args The standard Stalker.job args
  # @option [Integer] :pri The Job priority
  # @option [Integer] :delay Delay in seconds before job is ready to be reserved
  # @option [Integer] :ttr Number of seconds before the job will timeout after it has been reserved
  # @param [Boolean] beanstalk_style If true, job will have access to the Beanstalk::Job instance as a 3rd arg to Stalker.job
  #   Defaults to false
  # @param [Hash] style_opts If beanstalk_style is true, then style_opts has members that control variations on the Stalker.job lifecyle
  # @option [Boolean] :run_job_outside_of_stalker_timeout The job will be run outside of the 
  #   Stalker Timeout Only the Beanstalk::Job#ttr applies. If you use this mode, there can be no
  #   before_handlers for this job.
  # @option [Boolean] :explicit_delete If true, It will be up to your job to explicitly deltete, bury or release the Beanstalk::Job instance
  #   Default to not set (false)
  # @option [Booleasn] :no_bury_for_error_handler If true, AND there is an error handler in place, 
  #   Stalker will NOT bury the Beanstalk::Job if there is an Exception while the job is running
  #   Default is not set (false)
  #
  def enqueue(job, args={}, opts={}, beanstalk_style=false, style_opts={})
    pri   = opts[:pri]   || 65536
    delay = opts[:delay] || 0
    ttr   = opts[:ttr]   || 120
    beanstalk.use job
    beanstalk.put [ job, args, beanstalk_style, style_opts ].to_json, pri, delay, ttr
  rescue Beanstalk::NotConnected => e
    failed_connection(e)
  end

  
  def work_one_job
    job = beanstalk.reserve
    name, args, beanstalk_style, style_opts = JSON.parse job.body
    log_job_begin(name, args)

    handler = @@handlers[name]
    raise(NoSuchJob, name) unless handler

    if beanstalk_style
      run_beanstalk_style_job(job, name, args, handler, style_opts)
    else
      run_stalker_style_job(job, name, args, handler)
    end
    
  rescue Beanstalk::NotConnected => e
    failed_connection(e)
  rescue SystemExit
    raise
  rescue => e
    log_error exception_message(e)
    job.bury rescue nil unless style_opts[:no_bury_for_error_handler] && error_handler
    log_job_end(name, 'failed')
    if error_handler
      if error_handler.arity == 1
        error_handler.call(e)
      elsif error_handler.arity == 5
        error_handler.call(e, name, args, job, style_opts)
      else
        error_handler.call(e, name, args)
      end
    end
  end

  # Passes the Beanstalk::Job instance to the Stalker job as a second argument after args
  def run_beanstalk_style_job(job, name, args, handler, style_opts)
    unless style_opts['run_job_outside_of_stalker_timeout']
      begin
        Timeout::timeout(job.ttr - 1) do
          if defined? @@before_handlers and @@before_handlers.respond_to? :each
            @@before_handlers.each do |block|
              block.call(name)
            end
          end
            handler.call(args, job, style_opts) 
        end
      rescue Timeout::Error
        raise JobTimeout, "Stalker before_handlers for Stalker.job##{name} hit #{job.ttr-1}s timeout"
      end
    else
      handler.call(args, job, style_opts)
    end
    
    unless style_opts['explicit_delete']
      job.delete
      log_job_end(name)
    end
  end

  def run_stalker_style_job(job, name, args, handler)
    begin
      Timeout::timeout(job.ttr - 1) do
        if defined? @@before_handlers and @@before_handlers.respond_to? :each
          @@before_handlers.each do |block|
            block.call(name)
          end
        end
        handler.call(args)
      end
    rescue Timeout::Error
      raise JobTimeout, "#{name} hit #{job.ttr-1}s timeout"
    end

    job.delete
    log_job_end(name)
  end
end