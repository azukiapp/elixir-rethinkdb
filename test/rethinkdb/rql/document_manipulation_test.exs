defmodule Rethinkdb.Rql.DocumentManipulation.Test do
  use Rethinkdb.Case
  use Rethinkdb

  setup_all do
    {conn, table_name} = connect("doc_manipulation")
    table = r.table(table_name)
    table.insert([
      [id: "v1", value: "value 1", powers: 10],
      [id: "v2", value: "value 2", powers: 30],
    ]).run!(conn)
    {:ok, conn: conn, table: table }
  end

  test "use row in a array", var do
    {conn, _table} = {var[:conn], var[:table]}
    assert [2, 3] == r.expr([1, 2]).map(r.row.add(1)).run!(conn)
  end

  test "filter by row value", var do
    {conn, table} = {var[:conn], var[:table]}
    [result] = table.filter(r.row[:powers].gt(20)).run!(conn)
    assert "v2" == result[:id]
  end

  test "plucks a one or more attributes", var do
    {conn, table} = {var[:conn], var[:table]}
    data = [ k1: "k1", k2: [k3: "k3", k4: "k4"], k5: "k5"]

    result = table.get("v1").pluck(:value).run!(conn)
    assert Dict.has_key?(result, :value)
    refute Dict.has_key?(result, :id)

    result = r.expr(data).pluck([:k1, :k5]).run!(conn)
    assert Dict.has_key?(result, :k5)
    assert Dict.has_key?(result, :k1)
    refute Dict.has_key?(result, :k2)

    result = r.expr(data).pluck(k2: [k3: true]).run!(conn)
    assert Dict.has_key?(result, :k2)
    assert Dict.has_key?(result[:k2], :k3)
    refute Dict.has_key?(result[:k2], :k4)

    assert result == r.expr(data)
      .pluck(k2: [:k3]).run!(conn)
  end

  test "not take a attributes", var do
    {conn, table} = {var[:conn], var[:table]}
    data = [ k1: "k1", k2: [k3: "k3", k4: "k4"], k5: "k5"]

    result = table.get("v1").without(:value).run!(conn)
    refute Dict.has_key?(result, :value)
    assert Dict.has_key?(result, :id)

    result = r.expr(data).without([:k1, k2: [k3: true]]).run!(conn)
    refute Dict.has_key?(result, :k1)
    assert Dict.has_key?(result[:k2], :k4)
    refute Dict.has_key?(result[:k2], :k3)
  end

  test "merge a documents", var do
    {conn, table} = {var[:conn], var[:table]}

    final = HashDict.new(id: "v3", value: "value 3")
    assert final == r
      .expr([id: "v3"])
      .merge([value: "value 3"]).run!(conn)

    result = table.get("v1").merge(
      table.filter(id: "v2").with_fields([:value])[0]
    ).run!(conn)

    assert "v1" == result[:id]
    assert "value 2" == result[:value]
  end

  test "replace a nested object in merge", var do
    {conn, _table} = {var[:conn], var[:table]}

    result = r.expr(weapons: [
      'spectacular graviton beam': [
        dmg: 10, cooldown: 20
      ]
    ]).merge(weapons: r.literal(
      'repulsor rays': [
        dmg: 3, cooldown: 0
      ]
    )).run!(conn)

    assert Dict.has_key?(result[:weapons], :'repulsor rays')
    refute Dict.has_key?(result[:weapons], :'spectacular graviton beam')
  end

  test "use literal to remove keys from a object", var do
    {conn, _table} = {var[:conn], var[:table]}

    key = :'spectacular graviton beam'
    result = r.expr(weapons: [
      [{key, dmg: 10, cooldown: 20}]
    ]).merge(weapons:
      [{key, r.literal()}]
    ).run!(conn)

    assert Dict.has_key?(result[:weapons], key)
    assert nil == result[:weapons][key]
  end
end
