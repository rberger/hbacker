#!/usr/bin/env ruby
require "stalker"
require File.expand_path(File.join(File.dirname(__FILE__), "../", "lib", "stalker"))  

Stalker.enqueue('send.email', :email => 'hello@example.com')
Stalker.enqueue('cleanup.strays')
