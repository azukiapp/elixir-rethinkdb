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

  test "define a method to create a `not implemented error`" do
    message = "foobar not implemented yet"
    assert_raise RqlDriverError, message, fn ->
      RqlDriverError.not_implemented(:foobar)
    end
  end
end
