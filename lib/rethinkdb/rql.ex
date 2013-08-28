defmodule Rethinkdb.Rql do
  alias QL2.Datum
  alias QL2.Term

  defmacro __using__(_opts) do
    quote location: :keep do
      @behaviour Rethinkdb.Rql.Behaviour

      defrecordp :rql, __MODULE__, terms: []

      # TODO: Adding support initial expr
      @doc false
      def r, do: __MODULE__

      # ACCESSING RQL
      @doc false
      def run(conn, rql() = query) do
        conn._start(query)
      end

      # CONTROL STRUCTURES
      @doc false
      def expr(value) do
        rql(terms: Term.new(type: :'DATUM', datum: Datum.from_value(value)))
      end

      # Utils
      @doc false
      def build(rql(terms: terms)) do
        terms
      end

      @doc false
      defdelegate connect, to: Rethinkdb.Connection, as: :new
      @doc false
      defdelegate connect(opts), to: Rethinkdb.Connection, as: :new
    end
  end
end
