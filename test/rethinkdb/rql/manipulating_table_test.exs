defmodule Rethinkdb.Rql.ManipulatingTable.Test do
  use Rethinkdb.Case, async: false
  use Rethinkdb

  setup_all do
    {conn, table} = connect("dc_universe")
    {:ok, conn: conn, table: table }
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

  test "list table in database", var do
    {conn, name} = {var[:conn], var[:table]}
    assert name in r.db(conn.db).table_list.run!(conn)
  end

  test "create a new secondary simple index", var do
    {conn, name} = {var[:conn], var[:table]}
    table = r.table(name)
    table.index_drop("code_name").run(conn)

    assert HashDict.new(created: 1) ==
      table.index_create("code_name").run!(conn)
    assert "code_name" in table.index_list.run!(conn)
  end

  # TODO: Test index
  test "create a new secondary index with function", var do
    {conn, name} = {var[:conn], var[:table]}
    table = r.table(name)
    table.index_drop("power_rating").run(conn)

    assert HashDict.new(created: 1) == table.index_create("power_rating", fn hero ->
      hero["combat_power"].add(hero["compassion_power"].mul(2))
    end).run!(conn)
    assert "power_rating" in table.index_list.run!(conn)
  end

  test "table index drop", var do
    {conn, table} = {var[:conn], var[:table]}
    (table = r.table(table)).index_create("index_to_drop").run!(conn)
    assert HashDict.new(dropped: 1) ==
      table.index_drop("index_to_drop").run!(conn)
    refute ["index_to_drop"] == table.index_list.run!(conn)
  end
end
