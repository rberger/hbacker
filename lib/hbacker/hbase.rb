module Hbacker
  class HBase
    attr_reader :hbase_home, :hadoop_home
    
    def initialize(hbase_home, hadoop_home)
      @hbase_home = hbase_home
      @hadoop_home = hadoop_home
    end
  end
end

# ha = d.column_families.collect { |c| hsh = {}; c.instance_variables.collect { |i| k = i[1..-1].to_sym; v = c.instance_variable_get i; hsh.merge!(k => v)}; hsh}

#client.create_table("rob_test_please_delete", *ha)