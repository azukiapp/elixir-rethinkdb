defmodule Rethinkdb.Connection.Socket.Test do
  use Rethinkdb.Case, async: false

  alias Rethinkdb.Connection.Socket
  alias Rethinkdb.Connection.Options

  import Mock

  setup_all do
    {:ok, opts: Options.new}
  end

  test "open a socket with a opts", var do
    socket = Socket.connect!(var[:opts])
    assert is_record(socket, Socket)
    assert socket.open?
  end

  test "raise a Error to connection invalid" do
    assert_raise Socket.Error, "connection refused", fn ->
      Socket.connect!(Options.new(port: 1))
    end
  end

  test "open a socket with options", var do
    with_mock :gen_tcp, [:unstick, :passthrough], [] do
      socket = Socket.connect!(var[:opts])
      assert is_record(socket, Socket)
      assert called :gen_tcp.connect('localhost', var[:opts].port, packet: :raw)
      refute socket.close.open?
    end
  end

  test "set a pid process a controlling process of socket", var do
    with_mock :gen_tcp, [:unstick, :passthrough], [] do
      socket = Socket.connect!(var[:opts])
      assert socket == socket.process!(self)
      assert called :gen_tcp.controlling_process(:_, self)
      refute socket.close.open?
    end
  end

  test "raise a error in set controlling", var do
    pid    = spawn(fn() -> end)
    socket = Socket.connect!(var[:opts])

    assert_raise Socket.Error, "badarg", fn ->
      socket.process!(pid)
    end
  end

  test "check connection is open", var do
    socket = Socket.connect!(var[:opts])
    assert socket.open?
    refute socket.close.open?
  end

  test "send data to socket", var do
    with_mock :gen_tcp, [:unstick, :passthrough], [] do
      socket = Socket.connect!(var[:opts])
      assert :ok == socket.send(<<>>)
      assert called :gen_tcp.send(:_, <<>>)
      refute socket.close.open?
    end

    with_mock :gen_tcp, [:unstick, :passthrough], [] do
      socket = Socket.connect!(var[:opts])
      assert :ok == socket.send!(<<>>)
      assert called :gen_tcp.send(:_, <<>>)
      refute socket.close.open?
    end
  end

  test "return error if socket is closed", var do
    socket = Socket.connect!(var[:opts]).close
    assert {:error, :closed} == socket.send(<<>>)
    assert_raise Socket.Error, "Socket is closed", fn ->
      socket.send!(<<>>)
    end
  end
end
