defmodule Rethinkdb.Rql.Test do
  use Rethinkdb.Case, async: false
  use Rethinkdb

  alias Rethinkdb.Connection

  import Mock

  setup_all do
    conn = r.connect(db: "test")
    {:ok, conn: conn }
  end

  test "defines a terms to generate a ql2 terms" do
    rql  = r.expr(1)
    term = QL2.Term.new(type: :'DATUM', datum: QL2.Datum.from_value(1))
    assert term == rql.build
  end

  test "defines a run to forward Connection.run" do
    mocks = [
      connect!: fn _ -> {Connection} end,
      run: fn QL2.Term[] = term, {Connection} -> {:ok, term} end,
      run!: fn QL2.Term[] = term, {Connection} -> term end
    ]
    with_mock Connection, mocks do
      conn = r.connect
      assert {:ok, r.expr(1).build} == r.expr(1).run(conn)
      assert r.expr(1).build == r.expr(1).run!(conn)
    end
  end

  test "use default connection to execute a query" do
    r.connect.repl
    assert {:ok, 10} == r.expr(10).run
    assert 10 == r.expr(10).run!
  end

  test "return a connection with parameters" do
    conn = r.connect(host: "localhost")
    assert "localhost" == conn.options.host
    assert conn.open?

    conn = r.connect("rethinkdb://localhost")
    assert "localhost" == conn.options.host
    assert conn.open?
  end

  test "return a connection record" do
    conn = r.connect
    assert is_record(conn, Rethinkdb.Connection)
    assert conn.open?
  end
end

