require 'sqlite3'

module Selection
  def find_some(*ids)
   
    if ids.length == 1
      find_one(ids.first)
    else 
      ids.each do |id| 
        if id < 0 
          raise ArgumentError.new('ID invalid. ID must be a positive integer.')
        end 
      end 
      rows = connection.execute <<-SQL
        SELECT #{columns.join ","} FROM #{table}
        WHERE id IN (#{ids.join(",")});
      SQL

      rows_to_array(rows)
    end 
  end 


  def find_one(id)
    if id < 0 
      raise ArgumentError.new('ID invalid. ID must be a positive integer.')
    else 
      row = connection.get_first_row <<-SQL
        SELECT #{columns.join ","} FROM #{table}
        WHERE id = #{id};
      SQL

      init_object_from_row(row)
    end 
  end 

  def find_by(attribute, value)
    rows = connection.get_first_row <<-SQL
      SELECT #{columns.join ","} FROM #{table}
      WHERE #{attribute} = #{BlocRecord::Utility.sql_strings(value)};
    SQL

    rows_to_array(rows)
  end 

  def find_each(options={})
    rows = connection.execute <<-SQL
      SELECT #{columns.join ","} FROM #{table}
      LIMIT #{options[:batch_size]};
    SQL
     
    row_to_array(rows).each do |row|
      yield(row)
    end 
  end
  
  def find_in_batches(options={})
    rows = connection.execute <<-SQL
      SELECT #{columns.join ","} FROM #{table}
      LIMIT #{options[:batch_size]};
    SQL

    yield(rows_to_array(rows))
  end 

  def take_some(num=1)
    if num > 1
      rows = connection.execute <<-SQL 
        SELECT #{columns.join ","} FROM #{table}
        ORDER BY random()
        LIMIT #{num};
      SQL
      
      rows_to_array(rows)
    else 
      take_one 
    end 
  end 

  def take_one 
    row = connection.get_first_row <<-SQL
      SELECT #{columns.join ","} FROM #{table}
      ORDER BY random()
      LIMIT 1;
    SQL

    init_object_from_row(row)
  end 

  def first 
    row = connection.get_first_row <<-SQL 
      SELECT #{columns.join ","} FROM #{table}
      ORDER BY id ASC LIMIT 1;
    SQL

    init_object_from_row(row)
  end 

  def last 
    row = connection.get_first_row <<-SQL 
      SELECT #{columns.join ","} FROM #{table}
      ORDER BY id DESC LIMIT 1;
    SQL

    init_object_from_row(row)
  end 

  def all 
    rows = connection.execute <<-SQL
      SELECT #{columns.join ","} FROM #{table};
    SQL

    rows_to_array(rows)
  end
  
  def where(*args)
    if args.count > 1
      expression = args.shift
      params = args
    end

    sql = <<-SQL
      SELECT #{columns.join ","} FROM #{table}
      WHERE #{expression};
    SQL

    rows = connection.execute(sql, params)
    rows_to_array(rows)
  end

  private 

  def init_object_from_row(row)
    if row 
      data = Hash[columns.zip(row)]
      new(data)
    end 
  end 

  def rows_to_array(rows)
    rows.map { |row| new(Hash[columns.zip(row)]) }
  end 

  def method_missing(m, *args, &block)
    if m.match(/find_by_/)
      some_attribute_name = m.to_s.split('find_by_')[1]
      if columns.include?(some_attribute_name)
        find_by(some_attribute_name, *args)
      else 
        raise "#{m} is not a valid method."
      end 
    end 
  end
end 