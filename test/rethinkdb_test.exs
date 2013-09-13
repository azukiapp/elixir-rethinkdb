defmodule RethinkdbTest do
  use Rethinkdb.Case
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

  test "start is alias to application start" do
    with_mock Rethinkdb.App, [:passthrough], [] do
      :ok = Rethinkdb.start
      assert called Rethinkdb.App.start
    end
  end
end
