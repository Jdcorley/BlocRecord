require 'sqlite3'
require 'bloc_record/schema'

module Persistence
  def self.included(base)
    base.extend(ClassMethods)
  end

  def save
    save!
  rescue StandardError
    false
  end

  def save!
    unless id
      self.id = self.class.create(BlocRecord::Utility.instance_variables_to_hash(self)).id
      BlocRecord::Utility.reload_obj(self)
      return true
    end

    fields = self.class.attributes.map { |col| "#{col}=#{BlocRecord::Utility.sql_strings(instance_variable_get("@#{col}"))}" }.join

    self.class.connection.execute <<-SQL
      UPDATE #{self.class.table}
      SET #{fields}
      WHERE id = #{id};
    SQL

    true
  end

  def update_attributes(attribute, value)
    self.class.update(id, attribute => value)
  end

  def destroy
    self.class.destroy(self.id)
  end

  def update_attributes(updates)
    self.class.update(id, updates)
  end

  module ClassMethods
    def update_all(updates)
      update(nil, updates)
    end

    def destroy(*id)
      (id.length > 1) ? where_clause = "WHERE id IN (#{id.join(',')});" : where_clause = "WHERE id = #{id.first};"

      connection.execute <<-SQL
        DELETE FROM #{table} #{where_clause}
      SQL

      true
    end

    def create(attrs)
      attrs = BlocRecord::Utility.convert_keys(attrs)
      attrs.delete 'id'

      vals = attributes.map { |key| BlocRecord::Utility.sql_strings(attrs[key]) }

      connection.execute <<-SQL
        INSERT INTO #{table} (#{attributes.join ','})
        VALUES (#{vals.join ','});
      SQL

      data = Hash[attributes.zip attrs.values]
      data['id'] = connection.execute('SELECT last_insert_rowid();')[0][0]
      new(data)
    end

    def update(ids, updates)
      case updates
      when Hash
        updates = BlocRecord::Utility.convert_keys(updates)

        updates.delete 'id'
        updates_array = updates.map { |key, _value| "#{key}=#{BlocRecord::Utility.sql_strings(values)}" }

        where_clause = if ids.class == Integer
                         "WHERE id = #{ids};"
                       elsif ids.class == Array
                         ids.empty? ? ';' : "WHERE id IN (#{ids.join ','});"
                       else
                         ';'
                       end

        connection.execute <<-SQL
          UPDATE #{table}
          SET #{updates_array * ','}
        SQL

        true

      when Array
        updates.each_with_index { |data, index| update(ids[index], data) }
      end
    end

    def destroy_all(conditions_hash=nil)
      if conditions_hash && !conditions_hash.empty?
        conditions_hash = BlocRecord::Utility.convert_keys(conditions_hash)
        conditions = conditions_hash.map {|key, value| "#{key}=#{BlocRecord::Utility.sql_strings(value)}"}.join(" and ")

        connection.execute <<-SQL
           DELETE FROM #{table}
           WHERE #{conditions};
        SQL
      else
        connection.execute <<-SQL
          DELETE FROM #{table}
        SQL
      end

      true
    end
  end

  def method_missing(method, *args, &block)
    unless method != :update_name
      update(self.id, {name: *args[0]})
    end
  end
end
