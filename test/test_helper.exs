ExUnit.start

defmodule Rethinkdb.Case do
  use ExUnit.CaseTemplate
  use Rethinkdb

  using _ do
    quote do
      import unquote(__MODULE__)
      import Mock
    end
  end

  def default_timeout do
    System.get_env("WERCKER") && {:ok, 8000} ||
    :application.get_env(Mix.project[:app], :timeout)
  end

  def default_options do
    env_url = System.get_env("RETHINKDB_URL")
    {:ok, default} = :application.get_env(Mix.project[:app], :rethinkdb_uri)
    Rethinkdb.Connection.Options.new(env_url || default)
  end

  def default_connect do
    {:ok, timeout} = default_timeout
    options = default_options.timeout(timeout)
    r.connect(options).repl
    case r.db(options.db).info().run do
      {:ok, _} -> :ok
      _ ->
        r.db_create(options.db).run!
    end
  end

  # TODO: Fix meck bug in expectation with raise error
  defmacro mock_with_raise(mock_module, opts // [], mocks, test) do
    quote do
      :meck.new(unquote(mock_module), unquote(opts))
      Mock._install_mock(unquote(mock_module), unquote(mocks))
      try do
        unquote(test)
      after
        :meck.unload(unquote(mock_module))
      end
    end
  end

  defmacro save_repl(do: block) do
    quote do
      __conn = Rethinkdb.Connection.get_repl
      try do
        unquote(block)
      after
        __conn.repl
      end
    end
  end

  def table_to_test(table) do
    table_to_test(table, [])
  end

  def table_to_test(tables, opts) when is_list(tables) do
    lc table inlist tables do
      table_to_test(table, opts)
    end
  end

  def table_to_test(table, opts) do
    r.table_drop(table).run
    r.table_create(table, opts).run
    r.table(table)
  end

  # Debug in tests
  def pp(value), do: IO.inspect(value)
end

Rethinkdb.Case.default_connect
