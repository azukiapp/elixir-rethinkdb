defmodule Rethinkdb.Rql.StringManipulation.Test do
  use Rethinkdb.Case
  use Rethinkdb

  setup_all do
    {conn, table_name} = connect("string_manipulation")
    table = r.table(table_name)
    table.insert([
      [name: "Superman" , victories: 200],
      [name: "Batman", victories: 50],
    ]).run!(conn)
    {:ok, conn: conn, table: table }
  end

  test "return documents wherein anme match a regular expr", var do
    {conn, table} = {var[:conn], var[:table]}
    assert ["Batman"] == table.filter(fn hero ->
      hero[:name].match("^B")
    end)[:name].run!(conn)
  end

  test "does parse a string using a regex", var do
    {conn, _table} = {var[:conn], var[:table]}
    assert "mlucy" == r
      .expr("id:0,name:mlucy,foo:bar")
      .match("name:(\\w+)")[:groups][0][:str]
      .run!(conn)

    assert nil == r
      .expr("id:0,foo:bar")
      .match("name:(\\w+)")
      .run!(conn)
  end
end
