defmodule Rethinkdb.Rql.MathAndLogic do
  @moduledoc false

  defmacro __using__(_opts) do
    quote do
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
    end
  end
end
