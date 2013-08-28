defmodule Rethinkdb.Rql do
  alias QL2.Datum
  alias QL2.Term
  alias Rethinkdb.Utils

  defmacro __using__(_opts) do
    quote location: :keep do
      defrecordp :rql, __MODULE__, terms: []

      @type conn   :: Rethinkdb.Connection.t
      @type url    :: binary
      @type params :: Keyword.t
      @type term   :: Term.t

      @typep datum_arg :: :null | boolean | number | binary
      @typep expr_arg :: Dict.t | {any, any} | [expr_arg] | fun | atom | term | Term.AssocPair.t | datum_arg

      # TODO: Adding support initial expr
      @spec r :: atom
      def r, do: __MODULE__

      @doc false
      def rr, do: r

      @doc false
      def terms(rql(terms: terms)) do
        terms
      end

      @doc """
      Create a database. A RethinkDB database is a collection of tables,
      similar to relational databases.
      """

      # ACCESSING RQL
      def run(conn, rql(terms: terms) = query) do
        Utils.RunQuery.run(terms, conn)
      end

      def run!(conn, rql(terms: terms) = query) do
        Utils.RunQuery.run!(terms, conn)
      end

      # CONTROL STRUCTURES
      def expr(value) do
        rql(terms: Term.new(type: :'DATUM', datum: Datum.from_value(value)))
      end

      @doc """
      Create a new connection to the database server

      ## Example

      Opens a connection using the default host and port but specifying
      the default database.

          iex> conn = r.connect(db: "heroes")
      """
      @spec connect(params | url) :: conn
      def connect(opts // []) do
        Rethinkdb.Connection.new(opts).connect!
      end
    end
  end
end
