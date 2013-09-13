defmodule Rethinkdb.Connection do
  use GenServer.Behaviour

  alias Rethinkdb.RqlDriverError
  alias Rethinkdb.RqlRuntimeError

  alias Rethinkdb.Connection.Options
  alias Rethinkdb.Connection.Socket
  alias Rethinkdb.Connection.Supervisor
  alias Rethinkdb.Connection.Authentication

  alias QL2.Term
  alias QL2.Query
  alias QL2.Response

  defrecordp :conn, __MODULE__, pid: nil
  defrecord State, options: nil, socket: nil, next_token: 1

  @type t :: __MODULE__
  @type response :: success | error
  @type success  :: {:ok, any | [any]}
  @type error    :: {:error, any} | {:error, binary, atom, any}

  # To be used in repl
  @default_key :rethinkdb_default_connection

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

  @spec close(t) :: no_return
  def close(conn(pid: pid)) do
    monitor_ref = Process.monitor(pid)
    Process.exit(pid, :shutdown)
    receive do
      {:'DOWN', ^monitor_ref, :process, ^pid, _info} -> :ok
      other ->
        other
    end
  end

  # TODO: Using Process.wheries is not correct to do this
  @spec repl(t) :: t
  def repl(conn(pid: pid) = conn) do
    if Process.whereis(@default_key), do:
      Process.unregister(@default_key)
    Process.register(pid, @default_key)
    conn
  end

  @spec get_repl() :: t | {:error, String.t}
  def get_repl do
    case Process.whereis(@default_key) do
      nil -> {:error, "Not have a default connection" }
      pid -> conn(pid: pid)
    end
  end

  @spec open?(t) :: boolean
  def open?(conn(pid: pid)) do
    :gen_server.call(pid, :open?)
  end

  @spec options(t) :: Options.t
  def options(conn(pid: pid)) do
    :gen_server.call(pid, :options)
  end

  @spec db(t) :: String.t
  def db(conn(pid: pid)) do
    :gen_server.call(pid, :db)
  end

  @spec use(String.t, t) :: Option.t
  def use(database, conn(pid: pid) = conn) do
    :ok = :gen_server.cast(pid, {:use, database})
    conn
  end

  @spec run(Term.t, t) :: response
  def run(Term[] = query, conn(pid: pid)) do
    :gen_server.call(pid, {:run, query})
  end

  @spec run(Term.t) :: response
  def run(Term[] = query) do
    run(query, conn(pid: @default_key))
  end

  # TODO: Add test for error on the socket
  @spec run!(Term.t, t) :: any | [any] | no_return
  def run!(Term[] = query, conn() = conn) do
    case run(query, conn) do
      {:ok, response} -> response
      {:error, msg} when is_bitstring(msg) ->
        raise(RqlDriverError, msg: msg)
      {:error, type, msg, backtrace} ->
        raise(RqlRuntimeError, type: type, msg: msg, backtrace: backtrace)
    end
  end

  @spec run!(Term.t) :: any | [any] | no_return
  def run!(Term[] = query) do
    run!(query, conn(pid: @default_key))
  end

  # Supervisor API
  @spec start_link(Options.t) :: {:ok, pid}
  def start_link(Options[] = options) do
    :gen_server.start_link(__MODULE__, options, [])
  end

  # GenServer API
  @spec init(Options.t) :: {:ok, State.t} | { :stop, String.t }
  def init(Options[] = options) do
    Process.flag(:trap_exit, true)
    socket = Socket.connect!(options).process!(self)
    Authentication.auth!(socket, options)
    {:ok, State.new(options: options, socket: socket)}
  rescue
    x in [Socket.Error] ->
      { :stop, x.message }
  end

  def terminate(_reason, State[socket: socket]) do
    socket.close; :ok
  end

  def handle_call(:open?, _from, State[socket: socket] = state) do
    { :reply, socket.open?, state }
  end

  def handle_call(:options, _from, State[options: options] = state) do
    { :reply, options, state}
  end

  def handle_call(:db, _from, State[options: Options[db: db]] = state) do
    { :reply, db, state}
  end

  def handle_call({:run, Term[] = term}, _from,
    State[next_token: token, socket: socket, options: Options[db: db]] = state) do
    response = send_and_recv(Query.new_start(term, db, token), socket)
    { :reply, response, state.next_token(token + 1) }
  end

  def handle_cast({:use, database}, State[options: options] = state) do
    state = state.options(options.db(database))
    { :noreply, state }
  end

  def handle_info({:'EXIT', _from, reason}, state) do
    { :stop, reason, state }
  end

  defp send_and_recv(query, socket) do
    socket.send!(query.encode_to_send)
    Response.decode(recv(socket)).value
  rescue
    x in [Socket.Error] ->
      {:error, x.message}
  end

  defp recv(socket) do
    length = :binary.decode_unsigned(socket.recv!(4), :little)
    socket.recv!(length)
  end
end
