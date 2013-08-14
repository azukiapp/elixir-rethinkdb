defmodule Rethinkdb.ConnectionTest do
  use ExUnit.Case
  alias Rethinkdb.Connection

  test "check a default values for new connection" do
    conn = Connection.new

    assert "localhost" == conn.host
    assert 28015 == conn.port
    assert ""  == conn.authKey
    assert 20  == conn.timeout
    assert nil == conn.db
  end

  test "support create connect with uri" do
    conn = Connection.new("rethinkdb://remote:28106/rethinkdb_test")

    assert "remote" == conn.host
    assert 28106    == conn.port
    assert "rethinkdb_test" == conn.db

    conn = Connection.new("rethinkdb://remote:28106")
    assert nil == conn.db
  end

  test "return error for invalid uri connect" do
    {:error, msg} = Connection.new("")
    assert is_binary(msg)
    {:error, msg} = Connection.new("http://example.com")
    assert is_binary(msg)
  end
end

