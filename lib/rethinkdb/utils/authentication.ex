defmodule Rethinkdb.Utils.Authentication do
  alias Rethinkdb.Connection

  @version :binary.encode_unsigned(QL2.version, :little)

  # Send a protocol version and authenticate with key
  def authenticate(Connection[socket: socket, authKey: authKey] = conn) do
    authKey = [@version, <<iolist_size(authKey) :: [size(32), little]>>, authKey]
    :ok = socket.send(authKey)

    case read_until_null(socket) do
      {:ok, "SUCCESS"} -> {:ok, conn}
      {:ok, response} ->
        IO.puts("#{__MODULE__}.Error: #{response}")
        { :error, "Authentication to #{conn.host}:#{conn.port} fail with #{response}" }
    end
  end

  # Loop to recv and accumulate data from the socket
  defp read_until_null(socket, acc // <<>>) do
    result = << acc :: binary, socket.recv!(0) :: binary >>
    case String.slice(result, -1, 1) do
      << 0 >> ->
        {:ok, String.slice(result, 0, iolist_size(result) - 1) }
      _ -> read_until_null(socket, result)
    end
  end
end
