defmodule RethinkdbTest do
  use ExUnit.Case
  use Rethinkdb

  test "defined a function to get a AST" do
    assert Rethinkdb == r
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
