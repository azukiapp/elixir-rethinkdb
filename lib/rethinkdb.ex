defmodule Rethinkdb do
  alias Rethinkdb.Connection
  alias Rethinkdb.Rql

  defexception RqlDriverError, msg: nil, backtrace: nil do
    def message(RqlDriverError[msg: msg]) do
      msg
    end

    def not_implemented(method) do
      raise(RqlDriverError, msg: "#{method} not implemented yet")
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
    quote do
      import(Rql, only: [{unquote(method), 0}])
      alias unquote(__MODULE__).RqlDriverError
      alias unquote(__MODULE__).RqlRuntimeError
    end
  end
end
