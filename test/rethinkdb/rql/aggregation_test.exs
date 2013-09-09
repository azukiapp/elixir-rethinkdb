defmodule Rethinkdb.Rql.Aggregation.Test do
  use Rethinkdb.Case
  use Rethinkdb
  alias HashDict, as: H

  setup_all do
    {conn, table_name} = connect("aggregation")
    table = r.table(table_name)
    data  = [
      [id: "v1", group: "group1", power: 2, abilities: [primary: :yes]],
      [id: "v2", group: "group2", power: 3, abilities: [secundary: :no]],
      [id: "v3", group: "group2", power: 6, abilities: [primary: :yes]],
    ]
    table.insert(data, upsert: true).run!(conn)
    {:ok, conn: conn, table: table }
  end

  test "produce a single value from a sequence", var do
    {conn, table} = {var[:conn], var[:table]}
    reduce = fn acc, val -> acc.add(val) end
    assert 11 == table.map(r.row[:power]).reduce(reduce, 0).run!(conn)
    assert 06 == r.expr([2, 3]).reduce(reduce, 1).run!(conn)
  end

  test "remove duplicate elements from the sequence", var do
    {conn, _table} = {var[:conn], var[:table]}
    data = [ [power: [1, 2]], [power: [2, 3]] ]
    assert [1, 2] == r.expr([1, 2, 2, 1]).distinct.run!(conn)
    assert [1, 2, 3] == r.expr(data).concat_map(fn doc ->
      doc[:power]
    end).distinct.run!(conn)
  end

  test "group, map and reduce", var do
    {conn, table} = {var[:conn], var[:table]}
    [group1, group2] = table.grouped_map_reduce(
      fn doc -> doc[:group] end,
      fn doc -> doc.pluck([:id, :power]) end,
      fn acc, doc -> r.branch(doc[:power].lt(acc[:power]), doc, acc) end,
      id: "none", "power": 100
    ).run!(conn)
    assert H.new(id: "v1", power: 2.0) == group1[:reduction]
    assert H.new(id: "v2", power: 3.0) == group2[:reduction]
  end

  test "group elements by the values and sum", var do
    {conn, table} = {var[:conn], var[:table]}
    [group1, group2] = table.group_by(:group, r.sum(:power)).run!(conn)
    assert 2 == group1[:reduction]
    assert 9 == group2[:reduction]
  end

  test "group elements by the value and coutn", var do
    {conn, table} = {var[:conn], var[:table]}
    [group1, group2] = table.group_by(:group, r.count).run!(conn)
    assert 1 == group1[:reduction]
    assert 2 == group2[:reduction]
  end

  test "group by nested attributes and avg", var do
    {conn, table} = {var[:conn], var[:table]}
    [group1, group2] =
      table
      .group_by([abilities: [primary: true]], r.avg(:power))
      .run!(conn)
    assert 3 == group1[:reduction]
    assert 4 == group2[:reduction]
  end

  test "return element of sequences if the sequence has value", var do
    {conn, _table} = {var[:conn], var[:table]}
    data = ["superman", "ironman"]
    assert r.expr(data).contains("superman").run!(conn)
    assert r.expr(data).contains(data).run!(conn)
    refute r.expr(data).contains("spiderman").run!(conn)

    data = [
      ironman: [battles: [[winner: "ironman", loser: "superman"]]],
      superman: [battles: [[winner: "ironman", loser: "superman"]]],
    ]
    assert r.expr(data)[:ironman][:battles].contains(fn battle ->
      battle[:winner].eq("ironman")._and(battle[:loser].eq("superman"))
    end).run!(conn)
  end
end
