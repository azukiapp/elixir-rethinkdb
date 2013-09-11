defmodule Rethinkdb.Connection.Socket.Test do
  use Rethinkdb.Case, async: false
  alias Rethinkdb.Connection.Socket, as: Socket

  import Exmeck

  setup_all do
    {:ok, uri: "tcp://localhost:28015"}
  end

  test "open a socket with a uri", var do
    socket = Socket.connect!(var[:uri])
    assert is_record(socket, Socket)
    assert socket.open?
  end

  test "raise a Error to connection invalid" do
    assert_raise Socket.Error, "connection refused", fn ->
      Socket.connect!("tcp://localhost:1")
    end
  end

  test "open a socket with options", var do
    mock_connect fn mock ->
      args = ['localhost', 28015, packet: :raw]
      assert is_record(Socket.connect!(var[:uri]), Socket)
      assert 1 == mock.num_calls(:connect, args)
    end
  end

  test "set a pid process a controlling process of socket", var do
    mock_connect fn mock ->
      socket = Socket.connect!(var[:uri])
      assert socket == socket.process!(self)
      assert 1 == mock.num_calls(:controlling_process, [mock.module, self])
    end
  end

  test "raise a error in set controlling", var do
    pid    = spawn(fn() -> end)
    socket = Socket.connect!(var[:uri])

    assert_raise Socket.Error, "badarg", fn ->
      socket.process!(pid)
    end
  end

  test "check connection is open", var do
    socket = Socket.connect!(var[:uri])
    assert socket.open?
    refute socket.close.open?
  end

  test "send data to socket", var do
    mock_connect fn mock ->
      mock.stubs(:send, [mock.module, <<>>], :ok)
      socket = Socket.connect!(var[:uri])
      assert :ok == socket.send(<<>>)
      assert :ok == socket.send!(<<>>)
    end
  end

  test "return error if socket is closed", var do
    socket = Socket.connect!(var[:uri]).close
    assert {:error, :closed} == socket.send(<<>>)
    assert_raise Socket.Error, "Socket is closed", fn ->
      socket.send!(<<>>)
    end
  end

  def mock_connect(func) do
    mock_run :gen_tcp, [:unstick] do
      mock.stubs(:connect, [:_, :_, :_], {:ok, mock.module})
      mock.stubs(:controlling_process, [:_, :_], :ok)
      func.(mock)
    end
  end
end
