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
  def connect(socket_mod, rconn() = conn) do
    unless conn.socket do
      case socket_mod.connect conn.host, conn.port, packet: :raw, active: false do
        {:ok, socket} ->
          authenticate(rconn(conn, socket: socket))
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

  @doc """
    Close TCP socket connection
  """
  @spec close(t) :: no_return
  def close(rconn(socket: socket)), do: socket.close

  # Send a protocol version and authenticate with key
  defp authenticate(rconn(socket: socket, authKey: authKey) = conn) do
    :ok = socket.send(@version)

    authKey = [<<iolist_size(authKey) :: [size(32), little]>>, authKey]
    :ok = socket.send(authKey)

    case read_until_null(socket) do
      {:ok, <<"SUCCESS",0>>} -> {:ok, conn}
      {:ok, response} ->
        IO.puts("#{__MODULE__}.Error: #{response}")
        { :error, "Authentication to #{conn.host}:#{conn.port} fail with #{response}" }
    end
  end

  # Loop to recv and accumulate data from the socket
  defp read_until_null(socket, acc // <<>>) do
    result = << acc :: binary, socket.recv!(0) :: binary >>
    case String.slice(result, -1, 1) do
      << 0 >> -> {:ok, result }
      _ ->
        read_until_null(socket, result)
    end
  end
end
