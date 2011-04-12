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
require 'stargate'

module Stargate
  module Model
    class TableDescriptor < Record
      ##
      # Convert Stargate::Model::TableDescriptor#column_families into an array of hashes
      #
      def column_families_to_hashes
        self.column_families.collect do |c| 
          hsh = {}
          c.instance_variables.collect do |i| 
            k = i[1..-1].to_sym
            v = c.instance_variable_get i
            hsh.merge!(k => v)
          end
          hsh
        end
      end
    end
  end
  
  module Operation
    module TableOperation

      ##
      # Create an HBase table based on the Table Schema represented 
      # by a Stargate::Model::TableDescriptor instance
      # Will use TableDescriptor#name for the new table name unless 
      # the optional table_name argument is supplied
      
      def create_table_from_table_descriptor(table_descriptor, table_name=nil)
        table_name = table_descriptor.name unless table_name
        
        column_family_hashes = table_descriptor.column_families_to_hashes
        
        self.create_table(table_name, *column_family_hashes)
      end
    end
  end
end
