module Hbacker
  Dir[File.dirname(__FILE__) + '/lib/*.rb'].each {|file| require file }
  ##
  # Set Module up to have a Module Instance variable that will allow a logger to be shared by all classes in the Module
  # It needs to be initialized by one module. This will be done by Hbakcer::Cli.initialze
  #
  class << self
    require "logger"
    attr_accessor :log
  end
  unless @log
    @log = Logger.new(STDERR)
    @log.datetime_format = "%Y-%m-%d %H:%M:%S"
    @log.debug("Logger Active")
  end
end
