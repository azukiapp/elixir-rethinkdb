defmodule Rethinkdb.Connection do
  alias Socket.TCP
  alias Rethinkdb.Utils
  alias Rethinkdb.RqlDriverError, as: Error

  # Fields and default values for connection record
  @fields [ host: "localhost", port: 28015, authKey: "",
            timeout: 20, db: nil, socket: nil]

  # Record def
  Record.deffunctions(@fields, __ENV__)
  Record.import __MODULE__, as: :rconn

  @type t :: record

  @doc """
  Return a new connection according to the instructions
  of the uri.

  ## Example

      iex> #{__MODULE__}.new("rethinkdb://#{@fields[:host]}:#{@fields[:port]}/test")
      #{__MODULE__}[host: "localhost", port: 28015, authKey: nil, timeout: 20, db: "test"]
  """
  @spec new(binary) :: :conn.t

  def new(uri) when is_binary(uri) do
    case URI.parse(uri) do
      URI.Info[scheme: "rethinkdb", host: host, port: port, userinfo: authKey, path: db] ->
        db = List.last(String.split(db || "", "/"))
        rconn([
          host: host,
          port: port || @fields[:port],
          authKey: authKey || @fields[:authKey],
          db: db != "" && db || @fields[:db]
        ])
      _ ->
        {:error, "invalid uri, ex: rethinkdb://#{@fields[:authKey]}:#{@fields[:db]}/[database]"}
    end
  end

  @doc """
    Return true if connection is open
  """
  @spec open?(t) :: boolean
  def open?(rconn(socket: socket)) when socket != nil do
    case socket.local do
      {:ok, _ } -> true
      _ -> false
    end
  end

  def open?(_), do: false

  @doc """
    Create a TCP socket to the rethinkdb server, logs in and return
    a Connection record date with socket.
  """
  @spec connect(t) :: { :ok, t } | { :error, binary }
  def connect(rconn() = conn), do: conn.connect(Socket.TCP)

  @doc false
  def connect(socket_mod, rconn(socket: socket) = conn) when socket == nil do
    case socket_mod.connect(conn.host, conn.port, packet: :raw, active: false) do
      {:ok, socket} ->
        Utils.Authentication.authenticate(rconn(conn, socket: socket))
      {:error, _ } ->
        { :error, "Could not connect to #{conn.host}:#{conn.port}" }
    end
  end

  def connect(_socket_mod, rconn()) do
    {:error, "Apparently already connected, try to reconnect" }
  end

  @doc """
    Create a TCP socket to the rethinkdb server and return
    a Connection record date with socket, raising if
    an error occurs.
  """
  @spec connect!(t) :: t | no_return
  def connect!(rconn() = conn), do: conn.connect!(Socket.TCP)

  @doc false
  def connect!(socket_mod, rconn() = conn) do
    return_for_bang!(conn.connect(socket_mod))
  end

  @doc """
    Try to reconnect if the connection is closed and return a new
    connect record with new socket is success.
  """
  @spec reconnect(t) :: { :ok, t } | { :error, binary }
  def reconnect(rconn() = conn) do
    case open?(conn) do
      false -> connect(rconn(conn, socket: nil))
      true ->
        { :error, "Connection is open" }
    end
  end

  @doc """
    Try to reconnect if the connection is closed and return a new
    connect record with new socket is success. Otherwise raises an error.
  """
  @spec reconnect!(t) :: { :ok, t } | { :error, binary }
  def reconnect!(rconn() = conn) do
    return_for_bang!(reconnect(conn))
  end

  @doc """
  Close an open connection. Closing a connection cancels all outstanding requests
  and frees the memory associated with the open requests.
  """
  @spec close(t) :: no_return
  def close(rconn(socket: socket) = conn) do
    socket.close
    conn
  end

  @doc """
  Change the default database on this connection.

  ## Example:

  Change the default database so that we don't need to specify the database when
  referencing a table.

      iex> conn = Rethinkdb.connect
      iex> conn.use('heroes')
  """
  @spec use(binary, t) :: t
  def use(database, rconn() = conn) do
    rconn(conn, db: database)
  end
  @doc """
    Return new a unique token
  """
  def nextToken(rconn()) do
    :erlang.phash2({node(), make_ref})
  end

  # Ok or shoot exception?
  defp return_for_bang!({:ok, rconn() = conn}), do: conn
  defp return_for_bang!({:error, msg}), do: raise(Error, msg: msg)
end
