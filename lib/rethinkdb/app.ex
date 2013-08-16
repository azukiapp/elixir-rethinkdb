defmodule Rethinkdb.App do
  use Application.Behaviour

  def start do
    Application.Behaviour.start(:rethinkdb)
  end

  def start(_type, _args) do
    Rethinkdb.Supervisor.start_link
  end
end
