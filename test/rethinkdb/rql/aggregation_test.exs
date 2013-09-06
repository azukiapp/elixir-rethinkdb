defmodule Rethinkdb.Rql.Aggregation.Test do
  use Rethinkdb.Case
  use Rethinkdb

  setup_all do
    {conn, table_name} = connect("aggregation")
    table = r.table(table_name)
    data  = [
      [id: "v1", value: "value 1"],
      [id: "v2", value: "value 2"],
    ]
    table.insert(data, upsert: true).run!(conn)
    {:ok, conn: conn, table: table }
  end

  test "count documents", var do
    {conn, table} = {var[:conn], var[:table]}
    assert 2 == table.count.run!(conn)
    assert 4 == table.count.add(table.count).run!(conn)
  end

  test "count with filter", var do
    {conn, table} = {var[:conn], var[:table]}
    assert 1 == table.count(value: "value 1").run!(conn)
    assert 1 == table.count(r.row[:value].eq("value 1")).run!(conn)
  end
end
