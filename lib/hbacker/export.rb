module Hbacker
  class Export
    ##
    # Querys HBase to get a list of all the tables in the cluser
    # Iterates thru the list calling Export#table to do the Export to the specified dest
    #
    def all_tables(options)
      
    end
    
    ##
    # Iterates thru the list of tables calling Export#table to do the Export to the specified dest
    def specified_tables(options)
    end
    
    ##
    # Queries HBase for the table's schema
    # 
    def table
    end
  end
end
