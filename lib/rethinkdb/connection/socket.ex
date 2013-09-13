defmodule Rethinkdb.Connection.Socket do
  alias Rethinkdb.Connection.Options

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
  def connect!(Options[host: address, port: port]) do
    address = String.to_char_list!(address)

    opts = [:binary | [packet: :raw, active: false]]
    case :gen_tcp.connect(address, port, opts) do
      { :ok, socket }  -> record(socket: socket)
      { :error, code } -> raise Error, code: code
    end
  end

  @spec process!(pid, t) :: t | no_return
  def process!(pid, record(socket: socket) = record) do
    case :gen_tcp.controlling_process(socket, pid) do
      :ok -> record
      {:error, msg} -> raise Error, msg: msg
    end
  end

  @spec active!(t) :: :t | no_return
  def active!(mode // true, record(socket: socket) = record) do
    case :inet.setopts(socket, active: mode) do
      :ok -> record
      {:error, code} -> raise Error, code: code
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
      other -> error_raise(other)
    end
  end

  @spec recv!(number, t) :: binary
  def recv!(length // 0, timeout // :infinity, record(socket: socket)) do
    case :gen_tcp.recv(socket, length, timeout) do
      {:ok, data} -> data
      other -> error_raise(other)
    end
  end

  ## Loop to recv and accumulate data from the socket
  @spec recv_until_null!(number, t) :: binary
  def recv_until_null!(timeout, record() = record) when is_number(timeout) do
    recv_until_null!(<<>>, timeout, record)
  end

  @spec recv_until_null!(binary, number, t) :: binary
  defp recv_until_null!(acc, timeout, record() = record) do
    result = << acc :: binary, record.recv!(0, timeout) :: binary >>
    case String.slice(result, -1, 1) do
      << 0 >> ->
        String.slice(result, 0, iolist_size(result) - 1)
      _ -> recv_until_null!(result, timeout, record)
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

  defp error_raise({:error, :closed}) do
    raise Error, msg: "Socket is closed"
  end

  defp error_raise({:error, code}) do
    raise Error, code: code
  end
end
