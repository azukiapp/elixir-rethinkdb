defmodule Rethinkdb do
  alias Rethinkdb.Connection

  def connect, do: connect([])
  def connect(opts) do
    conn = Connection.new(opts)
  end
end
