defmodule RethinkdbTest do
  use ExUnit.Case
  alias Rethinkdb, as: R

  test "return a connection object to r" do
    conn = R.connect
    assert is_record(conn, R.Connection)
  end
end
