defmodule Rethinkdb do
  alias Rethinkdb.Connection

  def connect, do: connect([])
  def connect(opts) do
    Connection.new(opts)
  end
end
