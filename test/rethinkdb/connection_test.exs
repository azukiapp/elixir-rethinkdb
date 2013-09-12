defmodule Rethinkdb.Connection.Test do
  use Rethinkdb.Case, async: false

  alias Rethinkdb.Connection
  alias Rethinkdb.Connection.State
  alias Rethinkdb.Connection.Options
  alias Rethinkdb.Connection.Socket
  alias Rethinkdb.Connection.Supervisor

  import Mock

  def options, do: Options.new

  def mock_connect(mocks // []) do
    Dict.merge([
      connect!: fn _ -> {Socket} end,
      process!: fn _, {Socket} -> {Socket} end,
      open?:    fn {Socket} -> true end,
    ], mocks)
  end

  test "open socket and save in state" do
    opts = options
    {:ok, State[socket: socket, options: ^opts]} = Connection.init(options)
    assert socket.open?
    socket.close
  end

  test "stop if fail in connect" do
    assert {:stop, "connection refused"} ==
      Connection.init(Options.new(port: 1))
  end

  test "links the current process to the socket" do
    with_mock Socket, mock_connect do
      {:ok, State[]} = Connection.init(options)
      assert called Socket.process!(self, {Socket})
    end
  end

  test_with_mock "start connect with supervisor", Socket, mock_connect do
    with_mock Supervisor, [:passthrough], [] do
      {:ok, conn} = Connection.connect(options)
      assert is_record(conn, Connection)
      assert called Supervisor.start_worker(options)
    end

    with_mock Supervisor, [:passthrough], [] do
      conn = Connection.connect!(options)
      assert is_record(conn, Connection)
      assert called Supervisor.start_worker(options)
    end
  end

  test "bad connection opts return a error ou raise a exception" do
    opts = Options.new(port: 1)
    {:error, _} = Connection.connect(opts)

    assert_raise Rethinkdb.RqlDriverError, "Failed open connection", fn ->
      Connection.connect!(opts)
    end
  end

  test "return a socket status to call open?" do
    with_mock Socket, mock_connect do
      conn = Connection.connect!(options)
      assert conn.open?
      assert called Socket.open?({Socket})
    end
  end
end
