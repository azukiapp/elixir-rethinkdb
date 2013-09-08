defmodule Rethinkdb.Rql.Aggregation.Test do
  use Rethinkdb.Case
  use Rethinkdb

  setup_all do
    {conn, table_name} = connect("aggregation")
    table = r.table(table_name)
    data  = [
      [id: "v1", value: "value 1", power: 2],
      [id: "v2", value: "value 2", power: 2],
    ]
    table.insert(data, upsert: true).run!(conn)
    {:ok, conn: conn, table: table }
  end

  test "produce a single value from a sequence", var do
    {conn, table} = {var[:conn], var[:table]}
    reduce = fn acc, val -> acc.add(val) end
    assert 4 == table.map(r.row[:power]).reduce(reduce, 0).run!(conn)
    assert 6 == r.expr([2, 3]).reduce(reduce, 1).run!(conn)
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

  test "remove duplicate elements from the sequence", var do
    {conn, _table} = {var[:conn], var[:table]}
    data = [ [power: [1, 2]], [power: [2, 3]] ]
    assert [1, 2] == r.expr([1, 2, 2, 1]).distinct.run!(conn)
    assert [1, 2, 3] == r.expr(data).concat_map(fn doc ->
      doc[:power]
    end).distinct.run!(conn)
  end
end
