defmodule Rethinkdb.Connection.Supervisor do
  use Supervisor.Behaviour
  alias Rethinkdb.Connection
  alias Rethinkdb.Connection.Options

  # A convenience to start the supervisor
  def start_link() do
    :supervisor.start_link({:local, __MODULE__}, __MODULE__, [])
  end

  def init([]) do
    supervise [], strategy: :one_for_one
  end

  def start_worker(Options[] = options) do
    ref    = make_ref()
    worker = worker(Connection, [options], id: ref, restart: :temporary)
    :supervisor.start_child(
      __MODULE__, worker
    )
  end
end
