defmodule Rethinkdb.Utils.RunQuery.Test do
  use Rethinkdb.Case
  use Rethinkdb

  alias Rethinkdb.Utils.RunQuery
  alias Rethinkdb.Connection

  setup_all do
    {:ok, conn: Connection.new(db: "test").connect!}
  end

  test "return error for connection close" do
    conn  = Connection.new
    terms = QL2.Term.new
    msg   = "Connection is closed."

    assert {:error, msg} == RunQuery.run(terms, conn)
    assert_raise Rethinkdb.RqlDriverError, msg, fn ->
      RunQuery.run!(terms, conn)
    end
  end

  test "send a query to database", var do
    rql = r.expr(1)
    {:ok, response} = RunQuery.run(rql.terms, var[:conn])
    assert :'SUCCESS_ATOM' == response.type
  end

  test "send a query and receive response", var do
    rql = r.expr(1)
    response = RunQuery.run!(rql.terms, var[:conn])
    assert :'SUCCESS_ATOM' == response.type
  end
end
