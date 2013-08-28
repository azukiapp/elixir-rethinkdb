defmodule Rethinkdb.Utils.RunQuery.Test do
  use Rethinkdb.Case
  use Rethinkdb

  alias Rethinkdb.Utils.RunQuery

  setup_all do
    {:ok, conn: Rethinkdb.Connection.new(db: "test").connect!}
  end

  test "send a query to database", var do
    rql = r.expr(1)
    response = RunQuery.run(rql, var[:conn])
    assert :'SUCCESS_ATOM' == response.type
  end
end
