defmodule Rethinkdb.Rql.Aggregators.Test do
  use Rethinkdb.Case
  use Rethinkdb

  setup_all do
    {conn, table_name} = connect("aggregators")
    table = r.table(table_name)
    data  = [
      [id: "v1", power: 2],
      [id: "v2", power: 1],
    ]
    table.insert(data).run!(conn)
    {:ok, conn: conn, table: table }
  end

  test "count documents", var do
    {conn, table} = {var[:conn], var[:table]}
    assert 2 == r.expr([1, 3]).run!(conn)
    assert 2 == table.count.run!(conn)
    assert 4 == table.count.add(table.count).run!(conn)
  end

  test "count with filter", var do
    {conn, table} = {var[:conn], var[:table]}
    assert 1 == table.count(power: 2).run!(conn)
    assert 1 == table.count(r.row[:power].eq(1)).run!(conn)
  end

  #test "computer the sum", var do
    #{conn, table} = {var[:conn], var[:table]}
    #pp table.sum(:power).run!(conn)
  #end
end
