require 'thor'
require 'hbacker/export'
# require 'hbacker/import'

module Hbacker
  class CLI < Thor
    
    ##
    # Use (Now - 60 seconds) * 1000 to have a timestamp from 60 seconds ago in milliseconds
    #
    now_minus_60_sec = (Time.now.to_i - 60) * 1000

    desc "export", "Export HBase table[s]"
    method_option "incremental" :type => false, :aliases => "-i", 
      :banner => "Do an incremental export. Use stored last backup. Ignores --start"
    method_option :all => false, :aliases => "-a", 
      :banner => "All tables in HBase"
    method_option :tables => [], :aliases => "-t", 
      :banner => "Space separated list of tables"
    method_option "destination", :type => :string, :aliases => "-d", :required => true,
      :banner  => "Destination S3 bucket, S3n path, HDFS or File"
    method_option start => 0, :aliases => "-s", 
      :banner => "Start time (millisecs since Unix Epoch) Default: Shortly after Man first walked on Moon"
    method_option :end => now_minus_60_sec, :aliases => "-s", 
      :banner => "End time (millisecs since Unix Epoch) Default: Now - 60sec"
    def export
      puts "Export"
      puts "All: #{options[:all]}"
      puts "Tables: #{options[:tables].join(", ")}" if options[:tables]
      puts "Destination: #{options[:destination]}"
      if options[:all] && options[:tables]
        puts "Can only choose one of --all or --tables"
        help
        exit -1
      end
      if options[:all]
        HBacker::Export.all_tables options
      elsif options[:tables] && options[:destination]
        HBacker::Export.specified_tables options
      else
        puts "Invalid option combination"
        help
        exit -1
      end
    end
  end
end