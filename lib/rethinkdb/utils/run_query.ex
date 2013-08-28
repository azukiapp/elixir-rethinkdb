defmodule Rethinkdb.Utils.RunQuery do
  alias Rethinkdb.Connection
  alias Rethinkdb.RqlDriverError

  alias QL2.Term
  alias QL2.Query
  alias QL2.Response

  def run(Term[] = term, Connection[] = conn) do
    case conn.open? do
      true ->
        send_and_recv(new_query(term, conn), conn)
      false ->
        {:error, "Connection is closed."}
    end
  end

  def run!(terms, Connection[] = conn) do
    case run(terms, conn) do
      {:ok, response} -> response
      {:error, msg} when is_bitstring(msg) ->
        raise(RqlDriverError, msg: msg)
    end
  end

  defp send_and_recv(query, Connection[socket: socket]) do
    :ok = send(query, socket)
    {:ok, Response.decode(recv(socket))}
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

  defp new_query(Term[] = term, Connection[db: db] = conn) do
    Query.new(
      type: :'START',
      query: term,
      token: conn.nextToken,
      global_optargs: [QL2.global_database(db)]
    )
  end
end
