defmodule Rethinkdb.Rql.Aggregation.Test do
  use Rethinkdb.Case
  use Rethinkdb
  alias HashDict, as: H

  setup_all do
    table = table_to_test("aggregation")
    data  = [
      [id: "v1", group: "group1", power: 2, abilities: [primary: :yes]],
      [id: "v2", group: "group2", power: 3, abilities: [secundary: :no]],
      [id: "v3", group: "group2", power: 6, abilities: [primary: :yes]],
    ]
    table.insert(data, upsert: true).run!
    {:ok, table: table }
  end

  test "produce a single value from a sequence", var do
    reduce = fn acc, val -> acc.add(val) end
    assert 11 == var[:table].map(r.row[:power]).reduce(reduce, 0).run!
    assert 06 == r.expr([2, 3]).reduce(reduce, 1).run!
  end

  test "remove duplicate elements from the sequence" do
    data = [ [power: [1, 2]], [power: [2, 3]] ]
    assert [1, 2] == r.expr([1, 2, 2, 1]).distinct.run!
    assert [1, 2, 3] == r.expr(data).concat_map(fn doc ->
      doc[:power]
    end).distinct.run!
  end

  test "group, map and reduce", var do
    [group1, group2] = var[:table].grouped_map_reduce(
      fn doc -> doc[:group] end,
      fn doc -> doc.pluck([:id, :power]) end,
      fn acc, doc -> r.branch(doc[:power].lt(acc[:power]), doc, acc) end,
      id: "none", "power": 100
    ).run!
    assert H.new(id: "v1", power: 2.0) == group1[:reduction]
    assert H.new(id: "v2", power: 3.0) == group2[:reduction]
  end

  test "group elements by the values and sum", var do
    [group1, group2] = var[:table].group_by(:group, r.sum(:power)).run!
    assert 2 == group1[:reduction]
    assert 9 == group2[:reduction]
  end

  test "group elements by the value and coutn", var do
    [group1, group2] = var[:table].group_by(:group, r.count).run!
    assert 1 == group1[:reduction]
    assert 2 == group2[:reduction]
  end

  test "group by nested attributes and avg", var do
    [group1, group2] = var[:table]
      .group_by([abilities: [primary: true]], r.avg(:power))
      .run!
    assert 3 == group1[:reduction]
    assert 4 == group2[:reduction]
  end

  test "return element of sequences if the sequence has value" do
    data = ["superman", "ironman"]
    assert r.expr(data).contains("superman").run!
    assert r.expr(data).contains(data).run!
    refute r.expr(data).contains("spiderman").run!

    data = [
      ironman: [battles: [[winner: "ironman", loser: "superman"]]],
      superman: [battles: [[winner: "ironman", loser: "superman"]]],
    ]
    assert r.expr(data)[:ironman][:battles].contains(fn battle ->
      battle[:winner].eq("ironman")._and(battle[:loser].eq("superman"))
    end).run!
  end
end
