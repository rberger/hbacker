module Hbacker
  require "right_aws"
  require "hbacker"
  require "pp"
  
  class S3
    def initialize(aws_access_key_id, aws_secret_access_key)
      @s3 = RightAws::S3Interface.new(aws_access_key_id, aws_secret_access_key, {:logger => Hbacker.log}
    end
    
    def list_bucket_contents(bucket_name, path, options)
      @s3.incrementally_list_bucket(bucket_name,)
    end
  end
end
