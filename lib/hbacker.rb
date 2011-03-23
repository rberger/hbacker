module Hbacker
  require "rubygems"
  require "bundler/setup"
  ##
  # Set Module up to have a Module Instance variable that will allow a logger to be shared by all classes in the Module
  # It needs to be initialized by one module. This will be done by Hbakcer::Cli.initialze
  #
  require "logger"
  class << self
    attr_accessor :log
  end

  unless @log
    @log = Logger.new(STDERR)
    @log.datetime_format = "%Y-%m-%d %H:%M:%S"
    @log.level = Logger::WARN
  end

  Dir[File.dirname(__FILE__) + '/lib/*.rb'].each {|file| require file }
end
