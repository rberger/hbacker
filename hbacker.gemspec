# -*- encoding: utf-8 -*-
$:.push File.expand_path("../lib", __FILE__)
require "hbacker/version"

Gem::Specification.new do |s|
  s.name        = "hbacker"
  s.version     = Hbacker::VERSION
  s.platform    = Gem::Platform::RUBY
  s.authors     = ["Robert J. Berger Runa, Inc"]
  s.email       = ["rberger@runa.com"]
  s.homepage    = "http://blog.ibd.com"
  s.summary     = %q{Export and Import of HBase Cluster or individual tables}
  s.description = %q{Export and Import of HBase Cluster or individual tables using hadoop/hbase Mapreduce HBASE-1684}

  s.rubyforge_project = "hbacker"

  s.files         = `git ls-files`.split("\n")
  s.test_files    = `git ls-files -- {test,spec,features}/*`.split("\n")
  s.executables   = `git ls-files -- bin/*`.split("\n").map{ |f| File.basename(f) }
  s.require_paths = ["lib"]
  s.add_development_dependency "rspec", "~> 2.5.0"
  s.add_development_dependency "cucumber"
  s.add_development_dependency "aruba"
  s.add_dependency "thor"
  s.add_dependency "hbase-stargate"
  s.add_dependency "right_aws", ">= 2.0.0"
  s.add_dependency "uuidtools"
  s.add_dependency "stalker", "= 0.8.0"
end
