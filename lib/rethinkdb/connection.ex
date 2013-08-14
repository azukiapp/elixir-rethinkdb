defmodule Rethinkdb.Connection do

  # Fields and default values for connection record
  @fields [
    host: "localhost",
    port: 28015,
    authKey: "",
    timeout: 20,
    db: nil
  ]

  # Record def
  Record.deffunctions(@fields, __ENV__)
  Record.import __MODULE__, as: :conn

  @doc """
    Return a new connection according to the instructions
    of the uri.

    ## Examples

      iex> #{__MODULE__}.new("rethinkdb://localhost:28015/test")
      #{__MODULE__}[host: "localhost", port: 28015, authKey: nil, timeout: 20, db: "test"]
  """
  @spec new(binary) :: :conn.t

  def new(uri) when is_binary(uri) do
    default = conn
    case URI.parse(uri) do
      URI.Info[scheme: "rethinkdb", host: host, port: port, userinfo: authKey, path: db] ->
        db = List.last(String.split(db || "", "/"))
        conn([
          host: host, port: port,
          authKey: authKey || default.authKey,
          db: db != "" && db || default.db
        ])
      _ ->
        {:error, "invalid uri, ex: rethinkdb://#{default.host}:#{default.port}/[database]"}
    end
  end
end
