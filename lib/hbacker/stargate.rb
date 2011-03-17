require 'stargate'

module Stargate
  module Operation
    module TableOperation
      
      ##
      # Convert Stargate::Model::TableDescriptor#column_families into an array of hashes
      #
      def column_families_to_hash(column_families)
        column_families.collect do |c| 
          hsh = {}
          c.instance_variables.collect do |i| 
            k = i[1..-1].to_sym
            v = c.instance_variable_get i
            hsh.merge!(k => v)
          end
          hsh
        end
      end

      ##
      # Create an HBase table based on the Table Schema represented 
      # by a Stargate::Model::TableDescriptor instance
      # Will use TableDescriptor#name for the new table name unless 
      # the optional name argument is supplied
      
      def create_table_from_table_descriptor(table_descriptor, name=nil)
        table_name = table_descriptor.name unless name
        column_family_hashes = column_families_to_hash table_descriptor.column_families
        
        self.create_table(table_name, *column_family_hashes)
      end
    end
  end
end
