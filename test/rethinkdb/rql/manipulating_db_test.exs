defmodule Rethinkdb.Rql.ManipulatingDB.Test do
  use Rethinkdb.Case
  use Rethinkdb

  setup_all do
    {:ok, conn: r.connect, db: "#{dbns}_heroes"}
  end

  test "drop database", var do
    r.db_drop(var[:db]).run(var[:conn])
    assert_raise Rethinkdb.RqlRuntimeError, %r/Database `#{var[:db]}`/, fn ->
      r.db(var[:db]).info.run!(var[:conn])
    end
  end

  test "create database", var do
    r.db_drop(var[:db]).run(var[:conn])
    assert HashDict.new(created: 1) == r.db_create(var[:db]).run!(var[:conn])
  end

  test "list databases", var do
    r.db_drop(var[:db]).run(var[:conn])
    r.db_create(var[:db]).run(var[:conn])
    assert var[:db] in r.db_list.run!(var[:conn])
  end
end
