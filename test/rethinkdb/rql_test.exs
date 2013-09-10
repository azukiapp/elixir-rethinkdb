defmodule Rethinkdb.Rql.Test do
  use Rethinkdb.Case, async: false
  use Rethinkdb

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
    Exmeck.mock_run Rethinkdb.Utils.RunQuery do
      mock.stubs(:run, [:_, :_], {:ok, :result})
      mock.stubs(:run!, [:_, :_], :result)

      conn = r.connect
      rql  = r.expr(1)

      assert {:ok, :result} == rql.run(conn)
      assert 1 == mock.num_calls(:run, [rql.build, conn])

      assert :result == rql.run!(conn)
      assert 1 == mock.num_calls(:run!, [rql.build, conn])
    end
  end

  test "return a connection with parameters" do
    conn = r.connect(host: "localhost")
    assert "localhost" == conn.host
    assert conn.open?
    refute conn.close.open?

    conn = r.connect("rethinkdb://localhost")
    assert "localhost" == conn.host
    assert conn.open?
    refute conn.close.open?
  end

  test "return a connection record" do
    conn = r.connect
    assert is_record(conn, Rethinkdb.Connection)
    assert conn.open?
    refute conn.close.open?
  end
end

