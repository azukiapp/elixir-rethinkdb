defmodule Rethinkdb.Rql.ManipulatingTable.Test do
  use Rethinkdb.Case, async: false
  use Rethinkdb

  setup_all do
    conn = connect_with_db("#{dbns}_manipulatingtable")
    name = "dc_universe"
    try do
      r.table(name).run!(conn)
    rescue
      RqlRuntimeError ->
        r.table_create(name).run!(conn)
    end
    {:ok, conn: conn, table: name }
  end

  test "drop and create tables", var do
    {conn, table} = {var[:conn], var[:table]}

    assert HashDict.new(dropped: 1) ==
      r.db(conn.db).table_drop(table).run!(conn)
    assert HashDict.new(created: 1) ==
      r.db(conn.db).table_create(table).run!(conn)
  end

  test "create table with options", var do
    {conn, table} = {var[:conn], var[:table]}
    r.table_drop(table).run!(conn)

    assert HashDict.new(created: 1) ==
      r.table_create(table, primary_key: :name).run!(conn)
    assert "name" ==
      r.table(table).info.run!(conn)[:primary_key]

    assert_raise Rethinkdb.RqlRuntimeError, %r/Unrecognized/, fn ->
      r.table_create(table, invalid_key: 1).run!(conn)
    end
  end

  test "select table", var do
    {conn, name} = {var[:conn], var[:table]}

    table = r.table(name).info.run!(conn)
    assert name == table[:name]

    table = r.db(conn.db).table(name).info.run!(conn)
    assert name == table[:name]
  end

  test "list table in database", var do
    {conn, name} = {var[:conn], var[:table]}
    assert name in r.db(conn.db).table_list.run!(conn)
  end

  test "create a new secondary simple index", var do
    {conn, table} = {var[:conn], var[:table]}
    indexes = ["code_name", "power_rating"]
    table   = r.table(table)
    created = HashDict.new(created: 1)
    list    = table.index_list

    lc index inlist indexes, do: table.index_drop(index).run(conn)

    assert created == table.index_create("code_name").run!(conn)
    assert ["code_name"] == list.run!(conn)

    assert created == table.index_create("power_rating", fn hero ->
      hero["combat_power"].add(hero["compassion_power"].mul(2))
    end).run!(conn)

    assert indexes == list.run!(conn)
  end

  test "table index drop", var do
    {conn, table} = {var[:conn], var[:table]}
    (table = r.table(table)).index_create("index_to_drop").run!(conn)
    assert HashDict.new(dropped: 1) ==
      table.index_drop("index_to_drop").run!(conn)
    refute ["index_to_drop"] == table.index_list.run!(conn)
  end
end
