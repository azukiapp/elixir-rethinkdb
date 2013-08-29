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

      # MATH AND LOGIC
      @methods [
        "add", "sub", "mul", "div", "mod",
        {:and, :'ALL'}, {:or, :'ANY'},
        "eq", "ne", "gt", "ge", "lt", "le",
      ]

      def not(rql(terms: terms)) do
        rql(terms: Term.new(type: :'NOT', args: [terms]))
      end
      # CONTROL STRUCTURES
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

      # Define methods
      Module.eval_quoted __MODULE__, Enum.map(@methods, fn(logic) ->
        {logic, enum} = case logic do
          logic when is_tuple(logic) -> logic
          logic ->
            {:'#{logic}', :'#{String.upcase(logic)}'}
        end
        quote do
          def unquote(logic)(value, rql(terms: right)) do
            rql(terms: left) = expr(value)
            rql(terms: Term.new(type: unquote(enum), args: [right, left]))
          end
        end
      end)
    end
  end
end
