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

  def connect(table) do
    conn = r.connect(db: "#{dbns}")
    db   = r.db(conn.db)

    info_or_create(db, r.db_create(conn.db), conn)
    info_or_create(db.table(table), db.table_create(table), conn)

    {conn, table}
  end

  defp info_or_create(info, create, conn) do
    try do
      info.info.run!(conn)
    rescue
      RqlRuntimeError ->
        create.run!(conn)
    end
  end

  def pp(value), do: IO.inspect(value)
end
