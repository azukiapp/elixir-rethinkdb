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

      use Rethinkdb.Utils.RqlMethods

      # MANIPULATING DATABASES
      @doc """
      Create a database. A RethinkDB database is a collection of
      tables, similar to relational databases.

      If successful, the operation returns an object: HashDict#<[created: 1]>.
      If a database with the same name already exists the operation throws RqlRuntimeError.

      Example: Create a database named 'superheroes'.

          iex> r.db_create("superheroes").run!(conn)
          HashDict#<[created: 1]>
      """
      rql_method :db_create, :DB_CREATE, :primary

      @doc """
      Drop a database. The database, all its tables, and
      corresponding data will be deleted.

      If successful, the operation returns the object HashDict#<[dropped: 1]>.
      If the specified database doesn't exist a RqlRuntimeError is thrown.

      Example: Drop a database named 'superheroes'.

          iex> r.db_drop("superheroes").run!(conn)
          HashDict#<[dropped: 1]>
      """
      rql_method :db_drop, :DB_DROP, :primary

      @doc """
      List all database names in the system.

      The result is a list of strings.

      Example: List all databases.

          iex> r.db_list().run!(conn)
          ["test", "rethinkdb_test"]
      """
      rql_method :db_list, :DB_LIST, :primary_without

      # MANIPULATING TABLES
      rql_method :table_create, :TABLE_CREATE, :opts
      rql_method :table_drop, :TABLE_DROP
      rql_method :table_list, :TABLE_LIST, :without_param

      # ACCESSING RQL
      def run(conn, rql(terms: terms) = query) do
        Utils.RunQuery.run(terms, conn)
      end

      def run!(conn, rql(terms: terms) = query) do
        Utils.RunQuery.run!(terms, conn)
      end

      # SELECTING DATA
      rql_method :db, :DB, :primary

      # MATH AND LOGIC
      rql_method :add
      rql_method :sub
      rql_method :mul
      rql_method :div
      rql_method :mod

      rql_method :and, :'ALL'
      rql_method :or , :'ANY'
      rql_method :not, :'NOT', :without_param

      rql_method :eq
      rql_method :ne
      rql_method :gt
      rql_method :ge
      rql_method :lt
      rql_method :le

      # DOCUMENT MANIPULATION
      rql_method :append
      rql_method :prepend

      # CONTROL STRUCTURES
      rql_method :info, :'INFO', :without_param

      def expr(Term[] = terms), do: rql(terms: terms)
      def expr(rql() = query), do: query

      def expr([head|_] = value) when is_tuple(head) do
        expr(HashDict.new(value))
      end

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

      defp build_term_assocpair(key, value) when is_atom(key) do
        build_term_assocpair(atom_to_binary(key, :utf8), value)
      end
      defp build_term_assocpair(key, value) when is_binary(key) do
        Term.AssocPair.new(key: key, val: expr(value).terms)
      end

      defp option_term({key, value}) when is_atom(value) do
        option_term({key, atom_to_binary(value, :utf8)})
      end
      defp option_term({key, value}) when is_atom(key) do
        option_term({atom_to_binary(key, :utf8), value})
      end
      defp option_term({key, value}) do
        build_term_assocpair(key, value)
      end
    end
  end
end
