# Copyright 2011 Robert J. Berger & Runa, Inc.
# 
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
# 
#      http://www.apache.org/licenses/LICENSE-2.0
# 
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.
#    
module Hbacker
  require "rubygems"
  require "bundler/setup"
  require "pp"
  
  class HbackerError < RuntimeError; end
  
  ##
  # Set Module up to have a Module Instance variable that will allow a logger to be shared by all classes in the Module
  # It needs to be initialized by one module. This will be done by Hbakcer::Cli.initialze
  #
  require "logger"
  class << self
    # Way to access the Logger
    attr_accessor :log
  end

  unless @log
    @log = Logger.new(STDERR)
    @log.datetime_format = "%Y-%m-%d %H:%M:%S"
    @log.level = Logger::WARN
  end

  Dir[File.dirname(__FILE__) + '/hbacker/*.rb'].each {|file| require file unless file =~ /\/version.rb/ }
end
