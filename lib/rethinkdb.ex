defmodule Rethinkdb do
  alias Rethinkdb.Connection
  use Rethinkdb.Rql

  defexception RqlDriverError, msg: nil do
    def message(RqlDriverError[msg: msg]) do
      msg
    end
  end

  defexception RqlRuntimeError, msg: nil, type: nil, backtrace: nil do
    def message(RqlRuntimeError[msg: msg, type: type]) do
      "#{type}: #{msg}"
    end
  end

  defmacro __using__(_opts) do
    helper(__CALLER__.module)
  end

  # Import rr in Iex to not conflict Iex.Helper.r
  defp helper(module) do
    method = module && :r || :rr
    quote do: import(unquote(__MODULE__), only: [{unquote(method), 0}])
  end
end
