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

      Module.register_attribute __MODULE__, :methods, accumulate: true

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

      # MANIPULATING DATABASES
      @methods [
        {:db_create, :DB_CREATE, [:primary]},
        {:db_drop, :DB_DROP, [:primary]}
      ]

      # ACCESSING RQL
      def run(conn, rql(terms: terms) = query) do
        Utils.RunQuery.run(terms, conn)
      end

      def run!(conn, rql(terms: terms) = query) do
        Utils.RunQuery.run!(terms, conn)
      end

      # SELECTING DATA
      @methods [
        {:db, :DB, [:primary]}
      ]

      # MATH AND LOGIC
      @methods [
        "add", "sub", "mul", "div", "mod",
        {:and, :'ALL'}, {:or, :'ANY'}, {:not, :'NOT', [:without_param]},
        "eq", "ne", "gt", "ge", "lt", "le",
      ]

      # DOCUMENT MANIPULATION
      @methods [
        "append", "prepend"
      ]

      # CONTROL STRUCTURES
      @methods [
        {:info, :'INFO', [:without_param]}
      ]

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

      defmodule MethodHelpers do
        def get_methods(methods) do
          methods = List.flatten(methods)
          lc method inlist methods, do: method_def(method)
        end

        defp method_def({method, type, opts}) do
          quotes = []

          if :primary in opts do
            quotes = [method_def(:primary, method, type) | quotes]
          end

          if :without_param in opts do
            quotes = [method_def(:without_param, method, type) | quotes]
          end

          quotes
        end

        defp method_def(method) when is_bitstring(method) do
          method_def({:'#{method}', :'#{String.upcase(method)}'})
        end

        defp method_def({method, type})do
          quote do
            def unquote(method)(value, rql(terms: right)) do
              rql(terms: left) = expr(value)
              rql(terms: Term.new(type: unquote(type), args: [right, left]))
            end
          end
        end

        defp method_def(:primary, method, type) do
          quote do
            def unquote(method)(value) do
              args = [expr(value).terms]
              rql(terms: Term.new(type: unquote(type), args: args))
            end
          end
        end

        defp method_def(:without_param, method, type) do
          quote do
            def unquote(method)(rql(terms: right)) do
              rql(terms: Term.new(type: unquote(type), args: [right]))
            end
          end
        end
      end

      # Define methods
      Module.eval_quoted __MODULE__, MethodHelpers.get_methods(@methods)
    end
  end
end
