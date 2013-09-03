ExUnit.start

defmodule Rethinkdb.Case do
  use ExUnit.CaseTemplate
  use Rethinkdb

  using _ do
    quote do
      import unquote(__MODULE__)
      require Exmeck
    end
  end

  def dbns, do: "elixir_drive_test"

  def connect_with_db(db) do
    conn = r.connect(db: db)
    try do
      r.db(db).info.run!(conn)
    rescue
      RqlRuntimeError ->
        r.db_create(db).run!(conn)
    end
    conn
  end

  def pp(value), do: IO.inspect(value)
end
