defmodule Rethinkdb.Rql do
  # Geral record
  defrecordp :rql, __MODULE__, terms: []

  @type t      :: __MODULE__
  @type conn   :: Rethinkdb.Connection
  @type url    :: String.t
  @type params :: Keyword.t
  @type term   :: Term.t
  @type name   :: String.t | atom

  @type response :: success | error
  @type success  :: {:ok, any | [any]}
  @type error    :: {:error, binary, atom, any}

  @typep datum_arg :: :null | boolean | number | binary
  @typep expr_arg :: Dict.t | {any, any} | [expr_arg] | fun | atom | term | Term.AssocPair.t | datum_arg

  # TODO: Adding support initial expr
  @spec r :: module
  def r, do: __MODULE__

  @doc false
  def rr, do: r

  # Utils
  use Rethinkdb.Rql.TermsConstructor
  use Rethinkdb.Rql.Build

  # Rql methods
  use Rethinkdb.Rql.ManipulatingDatabases
  use Rethinkdb.Rql.ManipulatingTables
  use Rethinkdb.Rql.WriteData
  use Rethinkdb.Rql.SelectingData
  use Rethinkdb.Rql.Joins
  use Rethinkdb.Rql.Transformations
  use Rethinkdb.Rql.Aggregation
  use Rethinkdb.Rql.Aggregators
  use Rethinkdb.Rql.MathAndLogic
  use Rethinkdb.Rql.ManipulatingDocument
  use Rethinkdb.Rql.ManipulatingString
  use Rethinkdb.Rql.ControlStructures
  use Rethinkdb.Rql.Access

  @doc """
  Create a new connection to the database server,
  see `Connecton.new`.

  ## Example

  Opens a connection using the default host and port but specifying
  the default database.

      iex> conn = r.connect(db: "heroes")
  """
  @spec connect(params | url) :: conn
  def connect(opts // []) do
    Rethinkdb.Connection.new(opts).connect!
  end

  @doc """
  Run a query on a connection.

  ## Example ##

  Call run on the connection with a query to execute the query.

      iex> {:ok, heroes} = r.table("marvel").run(conn)
      iex> lc hero inlist heroes, do: IO.inspect(hero)
  """
  @spec run(conn, t) :: response
  def run(conn, rql() = query) do
    Rethinkdb.Utils.RunQuery.run(build(query), conn)
  end

  @doc """
  Run a query on a connection, raising if an error
  occurs, see `run`.
  """
  @spec run!(conn, t) :: any | [any] | no_return
  def run!(conn, rql() = query) do
    Rethinkdb.Utils.RunQuery.run!(build(query), conn)
  end
end
