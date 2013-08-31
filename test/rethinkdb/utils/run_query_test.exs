defmodule Rethinkdb.Utils.RunQuery.Test do
  use Rethinkdb.Case, async: false
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

  test "send a query and parse result", var do
    rql = r.expr(1)
    assert {:ok, 1.0} == RunQuery.run(rql.build, var[:conn])
    assert 1.0 == RunQuery.run!(rql.build, var[:conn])
  end

  test "raise error to response error" do
    Exmeck.mock_run do
      msg = "msg of error"
      mock_response(mock, QL2.Response.new(
        type: :'CLIENT_ERROR',
        response: [QL2.Datum.from_value(msg)],
        backtrace: QL2.Backtrace.new()
      ))

      assert_raise Rethinkdb.ResponseError, "CLIENT_ERROR: #{msg}", fn ->
        RunQuery.run!(r.expr(1).build, Connection.new(socket: mock.module))
      end
    end
  end

  defp mock_response(mock, response) do
    mock.stubs(:send!, [:_], :ok)
    mock.stubs(:local, [], {:ok, :local })
    mock.stubs(:recv!, fn
      4 -> <<20, 0, 0, 0>>
      _ -> response.encode
    end)
  end
end
