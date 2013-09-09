defmodule Rethinkdb.Rql.ManipulatingDocument do
  @moduledoc false

  defmacro __using__(_opts) do
    quote do
      def row do
        new_term(:'IMPLICIT_VAR', [])
      end

      def pluck(selector, rql() = query) do
        new_term(:'PLUCK', [selector], query)
      end

      def without(selector, rql() = query) do
        new_term(:'WITHOUT', [selector], query)
      end

      def merge(object, rql() = query) do
        new_term(:'MERGE', [expr(object)], [], query)
      end

      def literal(object // nil) do
        new_term(:'LITERAL', object && [object] || [])
      end

      def append(value, rql() = query) do
        new_term(:'APPEND', [value], query)
      end

      def prepend(value, rql() = query) do
        new_term(:'PREPEND', [value], query)
      end

      def difference(array, rql() = query) do
        new_term(:'DIFFERENCE', [array], query)
      end

      def set_insert(value, rql() = query) do
        new_term(:'SET_INSERT', [value], query)
      end

      def set_intersection(array, rql() = query) do
        new_term(:'SET_INTERSECTION', [array], query)
      end

      def set_difference(array, rql() = query) do
        new_term(:'SET_DIFFERENCE', [array], query)
      end

      def has_fields(selectors, rql() = query) do
        new_term(:'HAS_FIELDS', [selectors], query)
      end

      def insert_at(index, value, rql() = query) do
        new_term(:'INSERT_AT', [index, value], query)
      end

      def splice_at(index, array, rql() = query) do
        new_term(:'SPLICE_AT', [index, array], query)
      end

      def delete_at(index, endindex // nil, rql() = query) do
        new_term(:'DELETE_AT', [index | endindex && [endindex] || []], query)
      end

      def change_at(index, value, rql() = query) do
        new_term(:'CHANGE_AT', [index, value], query)
      end

      def keys(rql() = query) do
        new_term(:'KEYS', [], query)
      end
    end
  end
end

