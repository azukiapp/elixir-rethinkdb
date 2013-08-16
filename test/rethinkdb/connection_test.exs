defmodule Rethinkdb.ConnectionTest do
  use Rethinkdb.Case
  alias Rethinkdb.Connection

  import ExUnit.CaptureIO

  test "check a default values for new connection" do
    conn = Connection.new

    assert "localhost" == conn.host
    assert 28015 == conn.port
    assert ""    == conn.authKey
    assert 20    == conn.timeout
    assert nil   == conn.db
    assert nil   == conn.socket
  end

  test "support create connect with uri" do
    conn = Connection.new("rethinkdb://auth_key@remote:28106/rethinkdb_test")

    assert "remote" == conn.host
    assert 28106    == conn.port
    assert "rethinkdb_test" == conn.db
    assert "auth_key" == conn.authKey

    default = Connection.new
    conn    = Connection.new("rethinkdb://remote:28106")
    assert default.db == conn.db
    assert default.authKey == conn.authKey

    conn = Connection.new("rethinkdb://remote")
    assert default.port == conn.port
  end

  test "return error for invalid uri connect" do
    {:error, msg} = Connection.new("")
    assert is_binary(msg)
    {:error, msg} = Connection.new("http://example.com")
    assert is_binary(msg)
  end

  test "connect and logs in" do
    {:ok, conn} = Connection.new.connect
    assert is_record(conn, Connection)
    assert is_record(conn.socket, Socket.TCP)
    assert is_tuple(conn.socket.local!)

    conn = Connection.new.connect!
    assert is_record(conn, Connection)
    assert is_record(conn.socket, Socket.TCP)
    assert is_tuple(conn.socket.local!)
  end

  test "connect and authenticate with sucess" do
    conn = Connection.new("rethinkdb://auth_key@localhost")
    Exmeck.mock_run do
      mock_authenticate(mock)
      conn = conn.connect!(mock.module)

      version  = :binary.encode_unsigned(0x723081e1, :little)
      auth_key = "auth_key"
      auth_key = [<<iolist_size(auth_key) :: [size(32), little]>>, auth_key]

      args = [conn.host, conn.port, [
        packet: :raw,
        active: false
      ]]
      assert 1 = mock.num_calls(:connect, args)
      assert 1 = mock.num_calls(:send, [version])
      assert 1 = mock.num_calls(:send, [auth_key])
      assert 1 = mock.num_calls(:recv!, [0])
    end
  end

  test ":connect fail" do
    conn = Connection.new("rethinkdb://localhost:1")
    assert { :error, "Could not connect to #{conn.host}:#{conn.port}" } == conn.connect
  end

  test ":connect! fail" do
    conn = Connection.new("rethinkdb://localhost:1")
    assert_raise Connection.Error, fn ->
      conn.connect!
    end
  end

  test "authenticate fail" do
    Exmeck.mock_run do
      msg = "authenticate error"
      mock_authenticate(mock, <<msg :: binary,0>>)
      assert capture_io(fn ->
        assert_raise Connection.Error, %r/#{msg}/, fn ->
          Connection.new.connect!(mock.module)
        end
      end) =~ %r/#{msg}/
    end
  end

  test "receive data with loop" do
    Exmeck.mock_run do
      mock_authenticate(mock, fn _ ->
        mock.stubs(:recv!, [:_], << "SS", 0 >>)
        << "SUCCE" >>
      end)

      assert mock.module == Connection.new.connect!(mock.module).socket
    end
  end

  test "implements close" do
    conn = Connection.new.connect!
    assert is_tuple(conn.socket.local!)
    conn.close
    assert_raise Socket.TCP.Error, fn ->
      conn.socket.local!
    end
  end

  defp mock_authenticate(mock, response // <<"SUCCESS",0>>) do
    mock.stubs(:connect, [:_, :_, :_], {:ok, mock.module})
    mock.stubs(:send, [:_], :ok)
    if is_function(response) do
      mock.stubs(:recv!, response)
    else
      mock.stubs(:recv!, [:_], response)
    end
  end
end

