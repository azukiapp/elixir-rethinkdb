defmodule Rethinkdb.Rql.ManipulatingDB.Test do
  use Rethinkdb.Case
  use Rethinkdb

  def db, do: "elixir_drive_test_heroes"

  test "drop database" do
    r.db_drop(db).run
    assert_raise Rethinkdb.RqlRuntimeError, %r/Database `#{db}`/, fn ->
      r.db(db).info.run!
    end
  end

  test "create database" do
    r.db_drop(db).run
    assert HashDict.new(created: 1) == r.db_create(db).run!
  end

  test "list databases" do
    r.db_drop(db).run
    r.db_create(db).run
    assert db in r.db_list.run!
  end
end
