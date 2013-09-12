defmodule Rethinkdb.Connection.Authentication do
  alias Rethinkdb.Connection.Options
  alias Rethinkdb.Connection.Socket

  alias Rethinkdb.RqlDriverError

  @version :binary.encode_unsigned(QL2.version, :little)

  @spec auth!(Socket.t, Options.t) :: :ok | no_return
  def auth!(socket, Options[authKey: authKey] = options) when is_record(socket, Socket) do
    authKey = [@version, <<iolist_size(authKey) :: [size(32), little]>>, authKey]
    :ok = socket.send!(authKey)

    case socket.recv_until_null!() do
      "SUCCESS" -> :ok
      response ->
        raise RqlDriverError, msg:
          "Authentication to #{options.host}:#{options.port} fail with #{response}"
    end
  end
end
