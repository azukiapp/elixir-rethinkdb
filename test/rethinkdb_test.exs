defmodule RethinkdbTest do
  use ExUnit.Case
  alias Rethinkdb, as: R

  test "return a connection record" do
    conn = R.connect
    assert is_record(conn, R.Connection)
  end

  test "return a connection with parameters" do
    conn = R.connect(host: "example.com")
    assert "example.com" == conn.host

    conn = R.connect("rethinkdb://example.com")
    assert "example.com" == conn.host
  end
end
