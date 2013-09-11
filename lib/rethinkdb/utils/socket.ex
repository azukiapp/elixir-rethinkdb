defmodule Rethinkdb.Utils.Socket do
  alias Socket.TCP, as: TCP

  defrecordp :record, __MODULE__, socket: nil

  @type t :: __MODULE__
  @type tcp :: Socket.TCP

  # Open connect in passive mode
  @spec connect!(String.t | URI.Info.t) :: t | no_return
  def connect!(uri) when is_list(uri) or is_binary(uri) do
    connect!(URI.parse(uri))
  end

  def connect!(URI.Info[scheme: "tcp", host: host, port: port]) do
    socket = Socket.TCP.connect!(host, port, mode: :active)
    :ok = Socket.TCP.packet!(:raw, socket)
    record(socket: socket)
  end

  @spec open?(t) :: boolean
  def open?(record(socket: socket)) do
    case socket.local do
      {:ok, _ } -> true
      _ -> false
    end
  end

  @spec close(t) :: t
  def close(record(socket: socket) = record) do
    socket.close; record
  end

  @spec socket(t) :: tcp
  def socket(record(socket: socket)) do
    socket
  end
end
