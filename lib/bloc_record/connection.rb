require 'sqlite3'

module Connection
  def connection(type)
    case type
    when 'sqlite3'
      @connection ||= SQLite3::Database.new(BlocRecord.database_filename)
    when 'pg'
      @connection ||= PG::Connection.new(BlocRecord.database_filename)
    end
  end
end
