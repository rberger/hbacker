#!/usr/bin/env ruby
require "stalker"
require File.expand_path(File.join(File.dirname(__FILE__), "../", "lib", "stalker"))  

module Stalker
  # def log(msg); end
  # def log_error(msg); end
end

include Stalker

job 'send.email' do |args|
  log "Sending email to #{args['email']}"
end

job 'transform.image' do |args|
  log "Image transform"
end

job 'cleanup.strays' do |args|
  log "Cleaning up"
end

work
