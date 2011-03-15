require 'thor'
require 'hbacker/export'
# require 'hbacker/import'

module Hbacker
  class CLI < Thor
    desc "export [TABLES]", "Export HBase table[s]"
    method_option "all", :type => :boolean, :aliases => "-a", 
      :banner => "All tables in HBase"
    method_option "tables", :type => :array, :aliases => "-t", 
      :banner => "Space separated list of tables"
    method_option "destination", :type => :string, :aliases => "-d", :required => true,
      :banner  => "Destination S3 bucket, S3n path, HDFS or File"
    def export
      puts "Export"
      puts "All: #{options[:all]}"
      puts "Tables: #{options[:tables].join(", ")}" if options[:tables]
      puts "Destination: #{options[:destination]}"
      if options[:all] && options[:tables]
        stderr.puts "Can only choose one of --all or --tables"
        exit -1
      end
      HBacker::Export
    end
  end
end