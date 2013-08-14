defmodule Rethinkdb.Supervisor do
  use Supervisor.Behaviour

  def start_link do
    :supervisor.start_link({:local, __MODULE__}, __MODULE__, [])
  end

  def init([]) do
    Rethinkdb.Server = :ets.new(Rethinkdb.Server, [:ordered_set, :public, :named_table, {:read_concurrency, :true}])

    children = [
      # Define workers and child supervisors to be supervised
      worker(Rethinkdb.Server, [])
    ]

    # See http://elixir-lang.org/docs/stable/Supervisor.Behaviour.html
    # for other strategies and supported options
    supervise(children, strategy: :one_for_one)
  end
end
