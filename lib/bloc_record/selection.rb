require 'sqlite3'

module Selection
  def find_some(*ids)
    if ids.length == 1
      find_one(ids.first)
    else
      ids.each do |id|
        if id < 0
          raise ArgumentError, 'ID invalid. ID must be a positive integer.'
        end
      end
      rows = connection.execute <<-SQL
        SELECT #{columns.join ','} FROM #{table}
        WHERE id IN (#{ids.join(',')});
      SQL

      rows_to_array(rows)
    end
  end

  def find_one(id)
    if id < 0
      raise ArgumentError, 'ID invalid. ID must be a positive integer.'
    else
      row = connection.get_first_row <<-SQL
        SELECT #{columns.join ','} FROM #{table}
        WHERE id = #{id};
      SQL

      init_object_from_row(row)
    end
  end

  def find_by(attribute, value)
    rows = connection.get_first_row <<-SQL
      SELECT #{columns.join ','} FROM #{table}
      WHERE #{attribute} = #{BlocRecord::Utility.sql_strings(value)};
    SQL

    rows_to_array(rows)
  end

  def find_each(options = {})
    rows = connection.execute <<-SQL
      SELECT #{columns.join ','} FROM #{table}
      LIMIT #{options[:batch_size]};
    SQL

    row_to_array(rows).each do |row|
      yield(row)
    end
  end

  def find_in_batches(options = {})
    rows = connection.execute <<-SQL
      SELECT #{columns.join ','} FROM #{table}
      LIMIT #{options[:batch_size]};
    SQL

    yield(rows_to_array(rows))
  end

  def take_some(num = 1)
    if num > 1
      rows = connection.execute <<-SQL
        SELECT #{columns.join ','} FROM #{table}
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
      SELECT #{columns.join ','} FROM #{table}
      ORDER BY random()
      LIMIT 1;
    SQL

    init_object_from_row(row)
  end

  def first
    row = connection.get_first_row <<-SQL
      SELECT #{columns.join ','} FROM #{table}
      ORDER BY id ASC LIMIT 1;
    SQL

    init_object_from_row(row)
  end

  def last
    row = connection.get_first_row <<-SQL
      SELECT #{columns.join ','} FROM #{table}
      ORDER BY id DESC LIMIT 1;
    SQL

    init_object_from_row(row)
  end

  def all
    rows = connection.execute <<-SQL
      SELECT #{columns.join ','} FROM #{table};
    SQL

    rows_to_array(rows)
  end

  def where(*args)
    if args.count > 1
      expression = args.shift
      params = args
    else
      case args.first
      when String
        expression = args.first
      when Hash
        expression_hash = BlocRecord::Utility.convert_keys(args.first)
        expression = expression_hash.map { |key, value| "#{key}=#{BlocRecord::Utility.sql_strings(value)}" }.join(' and ')
      end
    end
  end

  def order(*args)
    if args.count > 1
      order = []
      args.map! do |x|
        x.map { |key, value| x = "#{key} #{value}" } unless x.class != Hash
        order << x
      end
      order = orders.join(', ')
    else
      if args[0].class == Hash
        order_hash = convert_keys(args[0])
        order = order_hash.map { |key, value| "#{key} #{sql_strings(value)}" }.join(', ')
      else
        order = args.first.join(', ')
      end
    end
    rows = connection.execute <<-SQL
        SELECT * FROM #{table}
        ORDER BY #{order_and_direction};
    SQL
    rows_to_array(rows)
  end

  def join(*args)
    if args.count > 1
      joins = args.map { |arg| "INNER JOIN #{arg} ON #{arg}.#{table}_id = #{table}.id" }.join(' ')
      rows = connection.execute <<-SQL
         SELECT * FROM #{table} #{joins}
      SQL
    else
      case args.first
      when String
        rows = connection.execute <<-SQL
           SELECT * FROM #{table} #{BlocRecord::Utility.sql_strings(args.first)};
        SQL
      when Symbol
        rows = connection.execute <<-SQL
           SELECT * FROM #{table}
           INNER JOIN #{args.first} ON #{args.first}.#{table}_id = #{table}.id
        SQL
      when Hash
        expression_hash = BlocRecord::Utility.convert_keys(args.first)
        expression = expression_hash.map do |key, value|
          "#{key}=#{BlocRecord::Utility.sql_strings(value)}".join(',')
        end
        rows = connection.execute <<-SQL
          SELECT * FROM #{table}
          INNER JOIN #{expression[0]} ON #{expression[0]}.#{table}_id = #{table}.id
          INNER JOIN #{expression[1]} ON #{expression[1]}.#{expression[0]}_id = #{table}.id
        SQL
      end
    end
    rows_to_array(rows)
  end
end

private

def init_object_from_row(row)
  if row
    data = Hash[columns.zip(row)]
    new(data)
  end
end

def rows_to_array(rows)
  collection = BlocRecord::Collection.new
  rows.each { |row| collection << new(Hash[columns.zip(row)]) }
  collection
end

def method_missing(m, *args)
  unless m !~ /find_by_/
    some_attribute_name = m.to_s.split('find_by_')[1]
    columns.include?(some_attribute_name) ? find_by(some_attribute_name, *args) : raise "#{m} is not a valid method."
  end
end
