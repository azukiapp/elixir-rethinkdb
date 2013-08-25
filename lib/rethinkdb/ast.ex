defmodule Rethinkdb.Ast do

  defrecordp :rql, __MODULE__, terms: []

  defmacro __using__(_opts) do
    quote do
      alias unquote(__MODULE__)
      import unquote(__MODULE__), only: [r: 0]
    end
  end

  defmacro r do
    quote do
      unquote(__MODULE__)
    end
  end

  # ACCESSING RQL
  def run(conn, rql() = query) do
    conn.run(query)
  end

  # CONTROL STRUCTURES
  def expr(expr_arg) do
    new_term(expr: [expr_arg])
  end

  defp new_term([{func, args}]) do
    rql(terms: [[func, args]])
  end
end
