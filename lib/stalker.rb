module Stalker
  extend self

  ##
  # Modified from Stalker 0.8.0 
  # Addes functionality to pass in the Beanstalk::Job instance to the Stalker.job Proc
  # Allows the Stalker.job Proc to access the Beanstalk::Job instance at the runtime of the Stalker.job Proc
  # The Stalker.job Proc can then get stats or touch the Beanstalk::Job instance (i.e. reset its timeout counter)
  #
  def work_one_job
    job = beanstalk.reserve
    name, args = JSON.parse job.body
    log_job_begin(name, args)
    
    # This is only diff from standard Stalker 0.8.0 R. Berger
    # Passes in the Beanstalk::Job instance to the Proc that is the actual code for the job to be executed
    # This gives that Proc the ability to access the Beanstalk::Job instance
    args.merge!({:job => job})
    
    handler = @@handlers[name]
    raise(NoSuchJob, name) unless handler

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
  rescue Beanstalk::NotConnected => e
    failed_connection(e)
  rescue SystemExit
    raise
  rescue => e
    log_error exception_message(e)
    job.bury rescue nil
    log_job_end(name, 'failed')
    if error_handler
      if error_handler.arity == 1
        error_handler.call(e)
      else
        error_handler.call(e, name, args)
      end
    end
  end
end