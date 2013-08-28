defmodule Rethinkdb.Rql.Behaviour do
  use Behaviour

  alias QL2.Datum
  alias QL2.Term

  @type rql    :: Rethinkdb.Rql.t
  @type conn   :: Rethinkdb.Connection.t
  @type url    :: binary
  @type params :: Keyword.t
  @type term   :: Term.t

  @typep datum_arg :: :null | boolean | number | binary
  @typep expr_arg :: Dict.t | {any, any} | [expr_arg] | fun | atom | term | Term.AssocPair.t | datum_arg

  defcallback r :: rql
  defcallback expr(expr_arg) :: rql
  defcallback build(rql) :: term

  @doc """
  Create a new connection to the dabatase server
  with default params.
  """
  defcallback connect :: conn

  @doc """
  Create a new connection to the database server

  ## Example

  Opens a connection using the default host and port but specifying
  the default database.

      iex> conn = r.connect(db: "heroes")
  """
  defcallback connect(params | url) :: conn
end
