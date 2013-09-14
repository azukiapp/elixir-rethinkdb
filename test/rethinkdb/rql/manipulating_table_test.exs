defmodule Rethinkdb.Rql.ManipulatingTable.Test do
  use Rethinkdb.Case, async: false
  use Rethinkdb

  setup_all do
    table_to_test(table = "dc_universe")
    {:ok, table: table }
  end

  test "drop and create tables", var do
    assert HashDict.new(dropped: 1) ==
      r.table_drop(var[:table]).run!
    assert HashDict.new(created: 1) ==
      r.table_create(var[:table]).run!
  end

  test "create table with options", var do
    table = var[:table]
    r.table_drop(table).run!

    assert HashDict.new(created: 1) ==
      r.table_create(table, primary_key: :name).run!
    assert "name" ==
      r.table(table).info.run![:primary_key]

    assert_raise Rethinkdb.RqlRuntimeError, %r/Unrecognized/, fn ->
      r.table_create(table, invalid_key: 1).run!
    end
  end

  test "list table in database", var do
    assert var[:table] in r.table_list.run!
  end

  test "create a new secondary simple index", var do
    table = r.table(var[:table])
    table.index_drop("code_name").run

    assert HashDict.new(created: 1) ==
      table.index_create("code_name").run!
    assert "code_name" in table.index_list.run!
  end

  # TODO: Test index
  test "create a new secondary index with function", var do
    table = r.table(var[:table])
    table.index_drop("power_rating").run

    assert HashDict.new(created: 1) == table.index_create("power_rating", fn hero ->
      hero["combat_power"].add(hero["compassion_power"].mul(2))
    end).run!
    assert "power_rating" in table.index_list.run!
  end

  test "table index drop", var do
    (table = r.table(var[:table])).index_create("index_to_drop").run!
    assert HashDict.new(dropped: 1) ==
      table.index_drop("index_to_drop").run!
    refute ["index_to_drop"] == table.index_list.run!
  end
end
