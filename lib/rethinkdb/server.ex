defmodule Rethinkdb.Server do
  use GenServer.Behaviour

  def start_link do
    :gen_server.start_link({:local, __MODULE__}, __MODULE__, [], [])
  end

  def stop do
    :gen_server.call(__MODULE__, :stop)
  end
end
