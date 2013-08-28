defmodule Rethinkdb.Rql.Test do
  use Rethinkdb.Case

  defmodule RqlTest do
    use Rethinkdb.Rql
  end

  def r, do: RqlTest

  test "defines a run to call build in connect" do
    Exmeck.mock_run do
      mock.stubs(:_start, fn rql -> rql end)

      conn = mock.module
      rql  = r.expr(1)

      assert rql == rql.run(conn)
      assert 1   == mock.num_calls(:_start, [rql])
    end
  end

  test "defines a build to generate a ql2 terms" do
    rql  = r.expr(1)
    term = QL2.Term.new(type: :'DATUM', datum: QL2.Datum.from_value(1))
    assert term == rql.build
  end

  test "return a connection with parameters" do
    conn = r.connect(host: "example.com")
    assert "example.com" == conn.host

    conn = r.connect("rethinkdb://example.com")
    assert "example.com" == conn.host
  end

  test "return a connection record" do
    conn = r.connect
    assert is_record(conn, Rethinkdb.Connection)
  end
end
