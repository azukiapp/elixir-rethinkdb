defimpl Access, for: Rethinkdb.Rql do
  def access(rql, key) do
    Rethinkdb.Rql.access(key, rql)
  end
end
