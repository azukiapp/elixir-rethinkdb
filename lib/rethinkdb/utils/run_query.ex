defmodule Rethinkdb.Utils.RunQuery do
  alias Rethinkdb.Connection

  def run(rql, Connection[db: db, socket: socket] = conn) do
    send_and_recv(socket, QL2.Query.new(
      type: :'START',
      query: rql.build,
      token: conn.nextToken,
      global_optargs: [QL2.global_database(db)]
    ))
  end

  defp send_and_recv(socket, query) do
    :ok = send(query, socket)
    QL2.Response.decode(recv(socket))
  end

  defp send(query, socket) do
    iolist = query.encode
    length = iolist_size(iolist)
    socket.send!([<<length :: [size(32), little]>>, iolist])
  end

  defp recv(socket) do
    length = :binary.decode_unsigned(socket.recv!(4), :little)
    socket.recv!(length)
  end
end
