defmodule Rethinkdb.Rql.Aggregators.Test do
  use Rethinkdb.Case
  use Rethinkdb

  setup_all do
    table = table_to_test("aggregators")
    data  = [
      [id: "v1", power: 2],
      [id: "v2", power: 1],
    ]
    table.insert(data).run!
    {:ok, table: table }
  end

  test "count documents", var do
    assert 2 == r.expr([1, 3]).count.run!
    assert 2 == var[:table].count.run!
    assert 4 == var[:table].count.add(var[:table].count).run!
  end

  test "count with filter", var do
    assert 1 == var[:table].count(power: 2).run!
    assert 1 == var[:table].count(r.row[:power].eq(1)).run!
  end
end
