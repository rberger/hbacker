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
  require "right_aws"
  require "hbacker"
  require 'fileutils'

  class S3
    attr_reader :s3
    # Initialize connection to S3
    # @param [String] aws_access_key_id Amazon Access Key ID
    # @param [String] aws_secret_access_key Amazon Secret Access Key
    #
    def initialize(aws_access_key_id, aws_secret_access_key)
      @s3 = RightAws::S3Interface.new(aws_access_key_id, aws_secret_access_key, {:logger => Hbacker.log})
    end
    
    ##
    # A more friendly way to list the contents of a bucket
    # @param [String] bucket_name The name of the bucket with no slashes
    # @param [String, nil] path Optional path to follow after bucket_name. 
    #   If not supplied, the result will be the top level contents of the bucket
    # @param [String, nil] delimiter Optional delimiter between components of the path. Defaults to '/'
    # @return [Array<String>] The list of directlry / filenames under the bucket_name/path
    #
    def list_bucket_contents(bucket_name, path = nil, delimiter = '/')
      list = []
      @s3.incrementally_list_bucket(bucket_name,{'prefix' => path, 'delimiter' => delimiter}) do |item|
        if item[:contents].empty?
          list << item[:common_prefixes]
        else
          list << item[:contents].map{|n| n[:key]}
        end
      end
      list.flatten
    end
    
    def save_info_to_file(bucket, key, data)
      full_path = "#{bucket}/#{key}"
      dir_path = File.dirname full_path
      Hbacker.log.debug "S3#save_info_to_file: dir_path: #{dir_path} full_path: #{full_path}"
      FileUtils.mkdir_p dir_path
      File.open(full_path, "w") do |f|
        f.write data
      end
    end
    
    def save_info(full_path, data)
      m = %r[(.*)://(.+?)/(.*)].match(full_path)
      protocol = m[1]
      bucket = m[2]
      key = m[3]
      if %w(s3 s3n).detect {|p| p == protocol }
        result = @s3.put(bucket, key, data)
      elsif protocol == "file"
        save_info_to_file(bucket, key, data)
      elsif protocol == "hdfs"
        dir_base = "HDFS_CMD_LOGS/#{bucket}"
        Hbacker.log.warn "Map/Reduce Job Logs will be stored in #{dir_base}/#{key}"
        save_info_to_file(dir_base, key, data)
      else
        msg = "Invalid protocol: #{protocol} for saving Map/Reduce Logs for #{full_path}"
        Hbacker.log.error msg
        raise msg
      end
    end
  end
end
