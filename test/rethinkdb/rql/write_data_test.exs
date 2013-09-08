defmodule Rethinkdb.Rql.WriteData.Test do
  use Rethinkdb.Case, async: false
  use Rethinkdb

  setup_all do
    {conn, table} = connect("marvel")
    {:ok, conn: conn, table: table }
  end

  test "insert list of tuple in table", var do
    {conn, table} = {var[:conn], var[:table]}

    data   = [superhero: "Iron Man", superpower: "Arc Reactor"]
    result = r.table(table).insert(data).run!(conn)
    assert 1 == result[:inserted]
    assert 1 == length(result[:generated_keys])
  end

  test "insert hashdict in table", var do
    {conn, table} = {var[:conn], var[:table]}

    data   = HashDict.new(superhero: "Ciclope", superpower: "Optic Blast")
    result = r.table(table).insert(data).run!(conn)
    assert 1 == result[:inserted]
    assert 1 == length(result[:generated_keys])
  end

  test "insert a multiples hashs", var do
    {conn, table} = {var[:conn], var[:table]}

    data = [
      [ superhero: "Wolverine", superpower: "Adamantium" ],
      HashDict.new(superhero: "Spiderman", superpower: "spidy sense")
    ]
    result = r.table(table).insert(data, durability: :soft).run!(conn)
    assert 2 == result[:inserted]
    assert 2 == length(result[:generated_keys])
  end

  test "insert a list of tuple and return data", var do
    {conn, table} = {var[:conn], var[:table]}

    data   = HashDict.new(superhero: "Mystique", superpower: "Metamorphoses")
    result = r.table(table).insert(data, return_vals: true).run!(conn)

    new_val = result[:new_val]
    assert data[:superhero] == new_val[:superhero]
    assert data[:superpower] == new_val[:superpower]
  end

  test "insert and overwriting", var do
    {conn, table} = {var[:conn], var[:table]}
    table   = r.table(table)
    data    = HashDict.new(superhero: "Hulk", superpower: "Greenish")
    result  = table.insert(data, return_vals: true).run!(conn)

    result = table.insert(result[:new_val], upsert: true, return_vals: true).run!(conn)
    assert 1 == result[:unchanged]

    new_val = Dict.put(result[:new_val], :superpower, "Super Strength")

    result = table.insert(new_val, upsert: true, return_vals: true).run!(conn)
    assert 1 == result[:replaced]
    assert data[:superpower] == result[:old_val][:superpower]
    assert new_val[:superpower] == result[:new_val][:superpower]
  end

  test "update json documents", var do
    {conn, table} = {var[:conn], var[:table]}
    table = r.table(table)
    data  = HashDict.new(superhero: "Thor", superpower: "Beautiful hair")
    hero  = table.insert(data, return_vals: true).run!(conn)[:new_val]

    result = table.get(hero[:id]).update([superpower: "Thor's Hammer"], return_vals: true).run!(conn)
    assert 1 == result[:replaced]
    assert "Thor" == result[:new_val][:superhero]
    assert "Thor's Hammer"  == result[:new_val][:superpower]
    assert result[:old_val][:superpower] != result[:new_val][:superpower]
  end

  test "update json document with function", var do
    {conn, table} = {var[:conn], var[:table]}
    table = r.table(table)
    data  = HashDict.new(superhero: "Thor", age: 30, superpower: "Beautiful hair")
    hero  = table.insert(data, return_vals: true).run!(conn)[:new_val]

    result = table.get(hero[:id]).update(fn hero ->
      [age: hero[:age].add(2)]
    end, return_vals: true).run!(conn)
    assert 1  == result[:replaced]
    assert 32 == result[:new_val][:age]
  end

  test "replace json document", var do
    {conn, table} = {var[:conn], var[:table]}
    table = r.table(table)
    data  = HashDict.new(superhero: "Thor", superpower: "Beautiful hair")
    hero  = table.insert(data, return_vals: true).run!(conn)[:new_val]

    new    = Dict.delete(Dict.put(hero, :age, 30), :superpower)
    result = table.get(hero[:id]).replace(new, non_atomic: true, return_vals: true).run!(conn)
    assert 1   == result[:replaced]
    assert 30  == result[:new_val][:age]
    assert nil == result[:new_val][:superpower]
  end

  test "replace json document with function merge", var do
    {conn, table} = {var[:conn], var[:table]}
    table = r.table(table)
    data  = HashDict.new(superhero: "Thor", superpower: "Beautiful hair")
    hero  = table.insert(data, return_vals: true).run!(conn)[:new_val]

    result = table.get(hero[:id]).replace(fn hero ->
      hero.merge([is_fav: true])
    end, return_vals: true).run!(conn)
    assert 1 == result[:replaced]
    assert result[:new_val][:is_fav]
    assert hero[:superhero] == result[:new_val][:superhero]
  end

  test "delete json document", var do
    {conn, table} = {var[:conn], var[:table]}
    table = r.table(table)
    data = [
      [ superhero: "Wolverine", superpower: "Adamantium" ],
      [ superhero: "Spiderman", superpower: "spidy sense" ]
    ]
    [hero, _] = table.insert(data).run!(conn)[:generated_keys]
    result    = table.get(hero).delete(return_vals: true).run!(conn)

    assert 1    == result[:deleted]
    assert hero == result[:old_val][:id]

    assert table.delete().run!(conn)[:deleted] >= 1
  end
end
