defmodule Rethinkdb.Connection2Test do
  use Rethinkdb.Case, async: false
  alias Rethinkdb.Connection2, as: Connection
  alias Rethinkdb.Server

  import ExUnit.CaptureIO

  test "check a default values for new connection" do
    conn = Connection.new

    assert "localhost" == conn.host
    assert 28015 == conn.port
    assert ""    == conn.authKey
    assert 20    == conn.timeout
    assert nil   == conn.db
    assert nil   == conn.id
  end

  test "support create connect with uri" do
    conn = Connection.new("rethinkdb://auth_key@remote:28106/rethinkdb_test")

    assert "remote" == conn.host
    assert 28106    == conn.port
    assert "rethinkdb_test" == conn.db
    assert "auth_key" == conn.authKey

    default = Connection.new
    conn    = Connection.new("rethinkdb://remote:28106")
    assert default.db == conn.db
    assert default.authKey == conn.authKey

    conn = Connection.new("rethinkdb://remote")
    assert default.port == conn.port
  end

  test "return error for invalid uri connect" do
    {:error, msg} = Connection.new("")
    assert is_binary(msg)
    {:error, msg} = Connection.new("http://example.com")
    assert is_binary(msg)
  end
end
