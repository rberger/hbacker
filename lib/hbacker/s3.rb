module Hbacker
  require "right_aws"
  require "hbacker"
  require "pp"
  
  class S3
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
    
    def save_info(full_path, data)
      m = %r[.*://(.+?)(/.*)].match(full_path)
      bucket = m[1]
      key = m[2]
      Hbacker.log.debug "S3#save_info: bucket: #{bucket.inspect} key: #{key.inspect}"
      @s3.put(bucket, key, data)
    end
  end
end
