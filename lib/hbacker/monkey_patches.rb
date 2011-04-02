class Hash
  #take keys of hash and transform those to a symbols
  def self.transform_keys_to_symbols(value)
    return value if not value.is_a?(Hash)
    hash = value.inject({}) do |memo,(k,v)|
      k = k.class == Symbol ? k : k.downcase.gsub(/\s+|-/, "_").to_sym
      memo[k] = Hash.transform_keys_to_symbols(v); memo
    end
    return hash
  end
end