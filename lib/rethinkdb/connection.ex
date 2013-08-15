defmodule Rethinkdb.Connection do
  alias Socket.TCP

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
    Create a TCP socket to the rethinkdb server and return
    a Connection record date with socket.
  """
  @spec connect(t) :: { :ok, t } | { :error, binary }
  def connect(rconn() = conn) do
    unless conn.socket do
      case TCP.connect conn.host, conn.port, packet: 0, active: false do
        {:ok, socket} ->
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
  def connect!(rconn() = conn) do
    case connect(conn) do
      { :ok, conn } -> conn
      { :error, msg } -> raise Error, msg: msg
    end
  end
end
