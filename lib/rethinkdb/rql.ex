defmodule Rethinkdb.Rql do
  alias QL2.Datum
  alias QL2.Term
  alias Rethinkdb.Utils

  defmacro __using__(_opts) do
    quote location: :keep do

      defrecordp :term, type: nil, args: [], optargs: []
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
      def db_create(name), do: new_term(:'DB_CREATE', [name])

      @doc """
      Drop a database. The database, all its tables, and
      corresponding data will be deleted.

      If successful, the operation returns the object HashDict#<[dropped: 1]>.
      If the specified database doesn't exist a RqlRuntimeError is thrown.

      Example: Drop a database named 'superheroes'.

          iex> r.db_drop("superheroes").run!(conn)
          HashDict#<[dropped: 1]>
      """
      def db_drop(name), do: new_term(:'DB_DROP', [name])

      @doc """
      List all database names in the system.

      The result is a list of strings.

      Example: List all databases.

          iex> r.db_list().run!(conn)
          ["test", "rethinkdb_test"]
      """
      def db_list, do: new_term(:'DB_LIST')

      # MANIPULATING TABLES
      # TODO: Test options
      def table_create(name, opts // [], rql() = query // rql()) do
        new_term(:'TABLE_CREATE', [name], opts, query)
      end

      def table_drop(name, rql() = query // rql()) do
        new_term(:'TABLE_DROP', [name], [], query)
      end

      def table_list(rql() = query // rql()) do
        new_term(:'TABLE_LIST', [], query)
      end

      # ACCESSING RQL
      def run(conn, rql() = query) do
        Utils.RunQuery.run(build(query), conn)
      end

      def run!(conn, rql() = query) do
        Utils.RunQuery.run!(build(query), conn)
      end

      # SELECTING DATA
      def db(name) do
        new_term(:'DB', [name])
      end

      def table(name, rql() = query // rql()) do
        new_term(:'TABLE', [name], query)
      end

      # MATH AND LOGIC
      def add(value, rql() = query) do
        new_term(:'ADD', [value], query)
      end

      def sub(value, rql() = query) do
        new_term(:'SUB', [value], query)
      end

      def mul(value, rql() = query) do
        new_term(:'MUL', [value], query)
      end

      def div(value, rql() = query) do
        new_term(:'DIV', [value], query)
      end

      def mod(value, rql() = query) do
        new_term(:'MOD', [value], query)
      end

      def _and(value, rql() = query) do
        new_term(:'ALL', [value], query)
      end

      def _or(value, rql() = query) do
        new_term(:'ANY', [value], query)
      end

      def _not(rql() = query) do
        new_term(:'NOT', [], query)
      end

      def eq(value, rql() = query) do
        new_term(:'EQ', [value], query)
      end

      def ne(value, rql() = query) do
        new_term(:'NE', [value], query)
      end

      def gt(value, rql() = query) do
        new_term(:'GT', [value], query)
      end

      def ge(value, rql() = query) do
        new_term(:'GE', [value], query)
      end

      def lt(value, rql() = query) do
        new_term(:'LT', [value], query)
      end

      def le(value, rql() = query) do
        new_term(:'LE', [value], query)
      end

      # DOCUMENT MANIPULATION
      def append(value, rql() = query) do
        new_term(:'APPEND', [value], query)
      end

      def prepend(value, rql() = query) do
        new_term(:'PREPEND', [value], query)
      end

      # CONTROL STRUCTURES
      def info(rql() = query) do
        new_term(:'INFO', [], query)
      end

      def expr(rql() = query), do: query
      def expr([head|_] = value) when is_tuple(head) do
        expr(HashDict.new(value))
      end

      def expr(value), do: new_term(:EXPR, [value])

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

      # Build a rql terms in a ql2 terms
      @doc false
      def build(rql(terms: terms)) do
        Enum.reduce terms, nil, build_terms(&1, &2)
      end

      defp build_terms(term(type: :'EXPR', args: [value]), _left) do
        Term.new(type: :'DATUM', datum: Datum.from_value(value))
      end

      defp build_terms(term(type: type, args: args, optargs: optargs), left) do
        args = lc arg inlist args do
          build_terms(term(type: :'EXPR', args: [arg]), nil)
        end

        if left != nil, do: args = [left | args]

        Term.new(type: type, args: args)
      end

      # Helper to terms create
      defp new_term(type, args // []) do
        new_term(type, args, [], rql())
      end

      defp new_term(type, args, nil) do
        new_term(type, args, [], rql())
      end

      defp new_term(type, args, rql() = query) do
        new_term(type, args, [], query)
      end

      defp new_term(type, args, opts) do
        new_term(type, args, opts, rql())
      end

      defp new_term(type, args, optargs, rql(terms: terms)) do
        rql(terms: terms ++ [term(type: type, args: args, optargs: optargs)])
      end
    end
  end
end
