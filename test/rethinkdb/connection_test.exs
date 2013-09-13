defmodule Rethinkdb.Connection.Test do
  use Rethinkdb.Case, async: false
  use Rethinkdb

  alias Rethinkdb.Connection
  alias Rethinkdb.Connection.State
  alias Rethinkdb.Connection.Options
  alias Rethinkdb.Connection.Socket
  alias Rethinkdb.Connection.Supervisor
  alias Rethinkdb.Connection.Authentication

  alias QL2.Query

  def options, do: Options.new

  def mock_socket(mocks // []) do
    Dict.merge([
      connect!: fn _ -> {Socket} end,
      process!: fn _, {Socket} -> {Socket} end,
      open?:    fn {Socket}    -> true end,
      send!:    fn _, {Socket} -> :ok end,
      recv_until_null!:  fn _, {Socket} -> "SUCCESS" end,
      close:    fn {Socket} -> {Socket} end,
    ], mocks)
  end

  test "stop if fail in connect" do
    assert {:stop, "connection refused"} ==
      Connection.init(Options.new(port: 1))
  end

  test "links the current process to the socket" do
    with_mock Socket, mock_socket do
      {:ok, State[]} = Connection.init(options)
      assert called Socket.process!(self, {Socket})
    end
  end

  test "authentication after connect" do
    with_mock Authentication, [:passthrough], [] do
      {:ok, State[]} = Connection.init(options)
      assert called Authentication.auth!(:_, options)
    end

    assert_raise RqlDriverError, fn ->
      Connection.init(Options.new(authKey: "foobar"))
    end
  end

  test_with_mock "start connect with supervisor", Socket, mock_socket do
    with_mock Supervisor, [:passthrough], [] do
      {:ok, conn} = Connection.connect(options)
      assert is_record(conn, Connection)
      assert called Supervisor.start_worker(options)
      conn.close
    end

    with_mock Supervisor, [:passthrough], [] do
      conn = Connection.connect!(options)
      assert is_record(conn, Connection)
      assert called Supervisor.start_worker(options)
      conn.close
    end
  end

  test "bad connection opts return a error ou raise a exception" do
    opts = Options.new(port: 1)
    {:error, _} = Connection.connect(opts)

    assert_raise RqlDriverError, "Failed open connection", fn ->
      Connection.connect!(opts)
    end
  end

  test "return a socket status to call open?" do
    with_mock Socket, mock_socket do
      conn = Connection.connect!(options)
      assert conn.open?
      assert called Socket.open?({Socket})
    end
  end

  test "return a options" do
    with_mock Socket, mock_socket do
      conn = Connection.connect!(options)
      assert options == conn.options
    end
  end

  test "return a default db to connection" do
    with_mock Socket, mock_socket do
      conn = Connection.connect!(options)
      assert options.db == conn.db
    end
  end

  test "change default database" do
    with_mock Socket, mock_socket do
      conn = Connection.connect!(options)
      conn = conn.use("other")
      assert options.db("other") == conn.options
    end
  end

  test "build a query and send to database" do
    term  = r.expr([1, 2, 3]).build
    query = Query.new_start(term, options.db, 1)

    with_mock Socket, [:passthrough], [] do
      conn  = Connection.connect!(options)
      assert {:ok, [1, 2, 3]} == conn.run(term)
      assert [1, 2, 3] == conn.run!(term)

      assert called Socket.send!(query.encode_to_send, :_)
      conn.close
    end
  end

  test "return a database error" do
    conn = Connection.connect!(options)

    {:error, :RUNTIME_ERROR, msg, QL2.Backtrace[]} = conn.run(r.expr(1).add("2").build)
    assert Regex.match?(%r/Expected type NUMBER.*/, msg)

    assert_raise RqlRuntimeError, %r/RUNTIME_ERROR.*/, fn ->
      conn.run!(r.expr(1).add("2").build)
    end

    conn.close
  end

  test "set a connection a default connection and support run with this" do
    save_repl do
      conn = Connection.connect!(options)
      assert conn == conn.repl
      assert conn == Connection.get_repl
      assert {:ok, 1} == Connection.run(r.expr(1).build)
      assert 1 == Connection.run!(r.expr(1).build)
      conn.close
    end
  end

  test "close socket in terminate" do
    with_mock Socket, mock_socket do
      conn = Connection.connect!(options)
      conn.close
      assert called Socket.close(:_)
    end
  end

  test "remove from repl after close" do
    save_repl do
      conn = Connection.connect!(options).repl
      assert conn == Connection.get_repl
      conn.close
      assert {:error, "Not have a default connection"} == Connection.get_repl
    end
  end

  test "forward a timeout to the socket" do
    with_mock Socket, [:passthrough], [] do
      conn = Connection.connect!(options)
      r.expr(10).run(conn)
      assert called Socket.recv!(:_, options.timeout * 1000, :_)
    end
  end
end
