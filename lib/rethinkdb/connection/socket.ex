defmodule Rethinkdb.Connection.Socket do

  defrecordp :record, __MODULE__, socket: nil
  @type t :: __MODULE__

  defexception Error, code: nil, msg: nil do
    @type t :: Error.t

    def message(Error[code: code, msg: nil]) do
      to_string(:inet.format_error(code))
    end

    def message(Error[msg: msg]) do
      to_string(msg)
    end
  end

  # Open connect in passive mode
  @spec connect!(String.t | URI.Info.t) :: t | no_return
  def connect!(uri) when is_list(uri) or is_binary(uri) do
    connect!(URI.parse(uri))
  end

  def connect!(URI.Info[scheme: "tcp", host: address, port: port]) do
    address = String.to_char_list!(address)

    case :gen_tcp.connect(address, port, [packet: :raw]) do
      { :ok, socket } ->
        record(socket: socket)
      { :error, code } ->
        raise Error, code: code
    end
  end

  @spec process!(pid, t) :: no_return
  def process!(pid, record(socket: socket) = record) do
    case :gen_tcp.controlling_process(socket, pid) do
      :ok -> record
      {:error, msg} ->
        raise Error, msg: msg
    end
  end

  @spec send(iodata, t) :: :ok | { :error, Error.t }
  def send(data, record(socket: socket)) do
    :gen_tcp.send(socket, data)
  end

  @spec send!(iodata, t) :: :ok | no_return
  def send!(data, record) do
    case send(data, record) do
      :ok -> :ok
      {:error, :closed} ->
        raise Error, msg: "Socket is closed"
      {:error, code} ->
        raise Error, code: code
    end
  end

  @spec open?(t) :: boolean
  def open?(record(socket: socket)) do
    case :inet.sockname(socket) do
      { :ok, _ } -> true
      _ -> false
    end
  end

  @spec close(t) :: t
  def close(record(socket: socket) = record) do
    :gen_tcp.close(socket); record
  end
end
