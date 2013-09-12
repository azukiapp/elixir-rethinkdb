defmodule Rethinkdb.Connection.Socket.Test do
  use Rethinkdb.Case, async: false

  alias Rethinkdb.Connection.Socket
  alias Rethinkdb.Connection.Options

  import Mock

  def options, do: Options.new

  setup_all do
    {:ok, opts: Options.new}
  end

  test "open a socket with a opts" do
    socket = Socket.connect!(options)
    assert is_record(socket, Socket)
    assert socket.open?
  end

  test "raise a Error to connection invalid" do
    assert_raise Socket.Error, "connection refused", fn ->
      Socket.connect!(Options.new(port: 1))
    end
  end

  test "open a socket with options" do
    with_mock :gen_tcp, [:unstick, :passthrough], [] do
      socket = Socket.connect!(options)
      socket_opts = [packet: :raw, active: false]

      assert is_record(socket, Socket)
      assert called :gen_tcp.connect('localhost', options.port, socket_opts)
      refute socket.close.open?
    end
  end

  test "set a pid process a controlling process of socket" do
    with_mock :gen_tcp, [:unstick, :passthrough], [] do
      socket = Socket.connect!(options)
      assert socket == socket.process!(self)
      assert called :gen_tcp.controlling_process(:_, self)
      refute socket.close.open?
    end
  end

  test "raise a error in set controlling" do
    pid    = spawn(fn() -> end)
    socket = Socket.connect!(options)

    assert_raise Socket.Error, "badarg", fn ->
      socket.process!(pid)
    end
  end

  test "check connection is open" do
    socket = Socket.connect!(options)
    assert socket.open?
    refute socket.close.open?
  end

  test "send data to socket" do
    with_mock :gen_tcp, [:unstick, :passthrough], [] do
      socket = Socket.connect!(options)
      assert :ok == socket.send(<<>>)
      assert called :gen_tcp.send(:_, <<>>)
      refute socket.close.open?
    end

    with_mock :gen_tcp, [:unstick, :passthrough], [] do
      socket = Socket.connect!(options)
      assert :ok == socket.send!(<<>>)
      assert called :gen_tcp.send(:_, <<>>)
      refute socket.close.open?
    end
  end

  test "return error if socket is closed" do
    socket = Socket.connect!(options).close
    assert {:error, :closed} == socket.send(<<>>)
    assert_raise Socket.Error, "Socket is closed", fn ->
      socket.send!(<<>>)
    end
  end

  test "call active! change socket to active mode" do
    with_mock :inet, [:unstick, :passthrough], [] do
      socket = Socket.connect!(options)
      assert socket == socket.active!
      assert called :inet.setopts(:_, [ active: true ])
    end

    assert_raise Socket.Error, "invalid argument", fn ->
      Socket.connect!(options).close.active!
    end
  end

  test "call active!(:once) change socket to active once mode" do
    with_mock :inet, [:unstick, :passthrough], [] do
      socket = Socket.connect!(options)
      assert socket == socket.active!(:once)
      assert called :inet.setopts(:_, [ active: :once ])
    end

    assert_raise Socket.Error, "invalid argument", fn ->
      Socket.connect!(options).close.active!(:once)
    end
  end

  test "aception only [true, :once] modes in active!" do
    assert_raise Socket.Error, "invalid argument", fn ->
      Socket.connect!(options).active!(1)
    end
    assert_raise Socket.Error, "invalid argument", fn ->
      Socket.connect!(options).active!(:twoce)
    end
  end

  test "recv and accumate data from the socket" do
    mocks = [
      recv: fn :socket, 0, :infinity ->
        :meck.expect(:gen_tcp, :recv, fn :socket, 0, :infinity ->
          {:ok, << "SS", 0 >> }
        end)
        {:ok, << "SUCCE" >>}
      end
    ]
    with_mock :gen_tcp, [:unstick], mock_socket(mocks) do
      socket = Socket.connect!(options)
      assert "SUCCESS" == socket.recv_until_null!
      assert called :gen_tcp.recv(:socket, 0, :infinity)
    end

    assert_raise Socket.Error, "Socket is closed", fn ->
      Socket.connect!(options).close.recv_until_null!
    end
  end

  test "recv with length and timeout options" do
    with_mock :gen_tcp, [:unstick], mock_socket do
      socket = Socket.connect!(options)
      socket.recv!(10)
      socket.recv!(10, 10)
      assert called :gen_tcp.recv(:socket, 10, :infinity)
      assert called :gen_tcp.recv(:socket, 10, 10)
    end

    assert_raise Socket.Error, "Socket is closed", fn ->
      Socket.connect!(options).close.recv!
    end
  end

  def mock_socket(mocks // []) do
    Dict.merge([
      connect: fn _, _, _ -> {:ok, :socket} end,
      recv: fn :socket, length, timeout ->
        {:ok, <<"binary:#{length}:#{timeout}">> }
      end,
    ], mocks)
  end
end
