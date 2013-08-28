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
    assert false == conn.open
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
    assert conn.open

    conn = Connection.new.connect!
    assert is_record(conn, Connection)
    assert is_record(conn.socket, Socket.TCP)
    assert conn.open
  end

  test "connect and authenticate with sucess" do
    conn = Connection.new("rethinkdb://auth_key@localhost")
    Exmeck.mock_run do
      mock_authenticate(mock)
      conn = conn.connect!(mock.module)

      version  = :binary.encode_unsigned(0x723081e1, :little)
      auth_key = "auth_key"
      auth_key = [version, <<iolist_size(auth_key) :: [size(32), little]>>, auth_key]

      args = [conn.host, conn.port, [
        packet: :raw,
        active: false
      ]]

      assert conn.socket == mock.module
      assert 1 = mock.num_calls(:connect, args)
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

  test "should not authenticate if is opened" do
    Exmeck.mock_run do
      mock_authenticate(mock)
      conn = Connection.new.connect!(mock.module)
      assert_raise Connection.Error, %r/try to reconnect/, fn ->
        conn.connect!
      end
    end
  end

  test "implements close" do
    conn = Connection.new.connect!
    assert is_tuple(conn.socket.local!)
    conn = conn.close
    refute conn.open
    assert_raise Socket.TCP.Error, fn ->
      conn.socket.local!
    end
  end

  test "support to reconnect and reconnect!" do
    e_msg = "Connection is open"

    {:ok, conn} = Connection.new.connect!.close.reconnect
    assert conn.open
    assert {:error, e_msg} == conn.reconnect

    conn = conn.close.reconnect!
    assert conn.open
    assert_raise Connection.Error, e_msg, fn ->
      conn.reconnect!
    end
  end

  test "defines `use` to change default database" do
    conn = Connection.new.db "test"
    assert "test" == conn.db

    conn = conn.use("test2")
    assert "test2" == conn.db
  end

  test "send a query to database" do
    term = QL2.Term.new(type: :'DATUM', datum: QL2.Datum.new(type: :'R_NUM', r_num: 1))
    Exmeck.mock_run do
      mock.stubs(:build, [], term)
      response = Connection.new(db: "test").connect!._start(mock.module)
      assert :'SUCCESS_ATOM' == response.type
    end
  end

  test "defined a method nextToken to return a unique token" do
    conn = Connection.new()
    {token1, token2} = {conn.nextToken, conn.nextToken}
    assert is_integer(token1)
    assert token1 != token2
  end

  defp mock_authenticate(mock, response // <<"SUCCESS",0>>) do
    mock.stubs(:connect, [:_, :_, :_], {:ok, mock.module})
    mock.stubs(:send, [:_], :ok)
    mock.stubs(:local, [], {:ok, :local })
    if is_function(response) do
      mock.stubs(:recv!, response)
    else
      mock.stubs(:recv!, [:_], response)
    end
  end
end

