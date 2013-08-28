defmodule Rethinkdb do
  alias Rethinkdb.Connection

  use Rethinkdb.Rql

  defmacro __using__(_opts) do
    helper(__CALLER__.module)
  end

  # Import rr in Iex to not conflict Iex.Helper.r
  defp helper(module) do
    method = module && :r || :rr
    quote do: import(unquote(__MODULE__), only: [{unquote(method), 0}])
  end
end
