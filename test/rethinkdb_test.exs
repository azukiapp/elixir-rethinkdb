defmodule RethinkdbTest do
  use ExUnit.Case
  use Rethinkdb

  test "defined a function to get a AST" do
    assert Rethinkdb.Rql == r
  end

  test "alias errors" do
    assert RqlDriverError  == Rethinkdb.RqlDriverError
    assert RqlRuntimeError == Rethinkdb.RqlRuntimeError
  end
end
