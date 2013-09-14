defmodule Rethinkdb.Connection.Options do
  # Fields and default values for connection record
  @fields [ id: nil, host: "localhost", port: 28015, authKey: "",
            timeout: 20, db: nil]

  # Record def
  Record.deffunctions(@fields, __ENV__)
  Record.import __MODULE__, as: :ropts

  @type t   :: __MODULE__
  @type uri :: String.t

  @doc """
  Return a new options according to the instructions
  of the uri.

  ## Example

      iex> #{__MODULE__}.new("rethinkdb://#{@fields[:host]}:#{@fields[:port]}/test")
      #{__MODULE__}[host: "localhost", port: 28015, authKey: nil, timeout: 20, db: "test"]
  """
  @spec new(uri) :: t
  def new(uri) when is_binary(uri) do
    extract_from_uri(URI.parse(uri))
  end

  @doc """
  Return a uri representation from a options record.

  ## Example

      iex> Options.new(db: "teste").to_uri
      "rethinkdb://#{@fields[:host]}:#{@fields[:port]}/test
  """
  @spec to_uri(t) :: String.t
  def to_uri(ropts(db: db, port: port, host: host, authKey: authKey)) do
    if authKey != nil do
      authKey = "#{authKey}@"
    end
    "rethinkdb://#{authKey}#{host}:#{port}/#{db}"
  end

  # New record from valid uri scheme
  defp extract_from_uri(URI.Info[
    scheme: "rethinkdb", host: host, port: port, userinfo: authKey, path: db
  ]) do
    db = List.last(String.split(db || "", "/"))
    ropts([
      host: host,
      port: port || @fields[:port],
      authKey: authKey || @fields[:authKey],
      db: db != "" && db || @fields[:db]
    ])
  end

  defp extract_from_uri(_) do
    {:error, "invalid uri, ex: rethinkdb://#{@fields[:authKey]}:#{@fields[:db]}/[database]"}
  end
end
