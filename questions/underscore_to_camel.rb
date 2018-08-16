def camelme(snake_case_word)
  string = snake_case_word.gsub!(/([_]+)/, ' ').split 
  string.each {|s| s.capitalize!}.join 
end 

puts camelme('snake_cased_word_to_camel')