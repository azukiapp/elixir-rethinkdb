defmodule Rethinkdb.Connection do
  alias Socket.TCP

  @version :binary.encode_unsigned(0x723081e1, :little)

  defexception Error, msg: nil do
    def message(Error[msg: msg]) do
      msg
    end
  end

  # Fields and default values for connection record
  @fields [
    host: "localhost",
    port: 28015,
    authKey: "",
    timeout: 20,
    db: nil,
    socket: nil
  ]

  # Record def
  Record.deffunctions(@fields, __ENV__)
  Record.import __MODULE__, as: :rconn

  @type t :: record

  @doc """
    Return a new connection according to the instructions
    of the uri.

    ## Examples

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
    Create a TCP socket to the rethinkdb server, logs in and return
    a Connection record date with socket.
  """
  @spec connect(t) :: { :ok, t } | { :error, binary }
  def connect(rconn() = conn), do: conn.connect(Socket.TCP)

  @doc false
  def connect(socket_mod, rconn(authKey: authKey) = conn) do
    unless conn.socket do
      case socket_mod.connect conn.host, conn.port, packet: :raw, active: false do
        {:ok, socket} ->
          :ok = authenticate(socket, authKey)
          {:ok, rconn(conn, socket: socket) }
        {:error, _ } ->
          { :error, "Could not connect to #{conn.host}:#{conn.port}" }
      end
    end
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
    case conn.connect(socket_mod) do
      { :ok, conn } -> conn
      { :error, msg } -> raise Error, msg: msg
    end
  end

  # Send a protocol version and authenticate keu
  defp authenticate(socket, auth_key) do
    :ok = socket.send(@version)

    auth_key = [<<iolist_size(auth_key) :: [size(32), little]>>, auth_key]
    socket.send(auth_key)
  end
end
