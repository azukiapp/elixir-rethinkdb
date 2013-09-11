defmodule Rethinkdb.Utils.Socket.Test do
  use Rethinkdb.Case
  alias Rethinkdb.Utils.Socket, as: SocketUtils

  test "open a socket with a uri" do
    uri    = "tcp://localhost:28015"
    socket = SocketUtils.connect!(uri)
    assert is_record(socket, SocketUtils)
    assert socket.open?
  end

  test "set options in connect" do
    Exmeck.mock_run Socket.TCP do
      mock.stubs(:connect!, [:_, :_, :_], mock.module)
      mock.stubs(:packet! , [:_, :_], :ok)
      uri  = "tcp://localhost:28015"
      args = ["localhost", 28015, mode: :active]

      assert is_record(SocketUtils.connect!(uri), SocketUtils)
      assert 1 == mock.num_calls(:connect!, args)
      assert 1 == mock.num_calls(:packet!, [:raw, mock.module])
    end
  end

  test "check connection is open" do
    uri    = "tcp://localhost:28015"
    socket = SocketUtils.connect!(uri)
    assert socket.open?
    refute socket.close.open?
  end
end
