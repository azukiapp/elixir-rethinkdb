defmodule Rethinkdb.Connection do
  use GenServer.Behaviour

  alias Rethinkdb.RqlDriverError
  alias Rethinkdb.Connection.Options
  alias Rethinkdb.Connection.Socket
  alias Rethinkdb.Connection.Supervisor

  defrecordp :conn, __MODULE__, pid: nil
  defrecord State, options: nil, socket: nil

  @type t :: __MODULE__

  # Public API
  @spec connect(Options.t) :: {:ok, t} | {:error, any}
  def connect(Options[] = options) do
    case Supervisor.start_worker(options) do
      {:ok, pid} when is_pid(pid) ->
        {:ok, conn(pid: pid)}
      other ->
        other
    end
  end

  @spec open?(t) :: boolean
  def open?(conn(pid: pid)) do
    :gen_server.call(pid, :open?)
  end

  @spec connect!(Options.t) :: t | no_return
  def connect!(Options[] = options) do
    case connect(options) do
      {:ok, conn} -> conn
      {:error, error} ->
        raise RqlDriverError,
          msg: "Failed open connection",
          backtrace: error
    end
  end

  # Supervisor API
  @spec start_link(Options.t) :: {:ok, pid}
  def start_link(Options[] = options) do
    :gen_server.start_link(__MODULE__, options, [])
  end

  # GenServer API
  @spec init(Options.t) :: {:ok, State.t} | { :stop, String.t }
  def init(Options[] = options) do
    socket = Socket.connect!(options).process!(self)
    {:ok, State.new(options: options, socket: socket)}
  rescue
    x in [Socket.Error] ->
      { :stop, x.message }
  end

  def handle_call(:open?, _from, State[socket: socket] = state) do
    { :reply, socket.open?, state }
  end
end
