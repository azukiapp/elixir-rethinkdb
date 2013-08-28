defmodule Rethinkdb.Rql.Test do
  use Rethinkdb.Case, async: false

  defmodule RqlTest do
    use Rethinkdb.Rql
  end

  def r, do: RqlTest

  test "defines a terms to generate a ql2 terms" do
    rql  = r.expr(1)
    term = QL2.Term.new(type: :'DATUM', datum: QL2.Datum.from_value(1))
    assert term == rql.terms
  end

  test "defines a run to call run with connect" do
    Exmeck.mock_run Rethinkdb.Utils.RunQuery do
      mock.stubs(:run, [:_, :_], {:ok, :result})
      mock.stubs(:run!, [:_, :_], :result)

      conn = r.connect
      rql  = r.expr(1)

      assert {:ok, :result} == rql.run(conn)
      assert 1 == mock.num_calls(:run, [rql.terms, conn])

      assert :result == rql.run!(conn)
      assert 1 == mock.num_calls(:run!, [rql.terms, conn])
    end
  end

  test "return a connection with parameters" do
    conn = r.connect(host: "localhost")
    assert "localhost" == conn.host

    conn = r.connect("rethinkdb://localhost")
    assert "localhost" == conn.host
  end

  test "return a connection record" do
    conn = r.connect
    assert is_record(conn, Rethinkdb.Connection)
  end
end

