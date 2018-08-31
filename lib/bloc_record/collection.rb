module BlocRecord
  class Collection < Array
    def update_all(updates)
      ids = map(&:id)
      any? ? first.class.update(ids, updates) : false
    end
  end
end
