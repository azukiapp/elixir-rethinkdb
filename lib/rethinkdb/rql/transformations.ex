defmodule Rethinkdb.Rql.Transformations do
  @moduledoc false

  defmacro __using__(_opts) do
    quote do
      def map(rql() = predicate, rql() = query) do
        map(fn _ -> predicate end, query)
      end

      def map(func, rql() = query) do
        new_term(:'MAP', [func(func)], query)
      end

      def with_fields(fields, options // [], rql() = query) do
        new_term(:'WITH_FIELDS', [fields, options], query)
      end

      def concat_map(func, rql() = query) do
        new_term(:'CONCATMAP', [func(func)], query)
      end

      def order_by([{:index, _}|_] = opts, rql() = query) do
        order_by([], opts, query)
      end

      def order_by(keys_or_function, opts // [], rql() = query) do
        if is_function(keys_or_function) do
          keys_or_function = func(keys_or_function)
        end
        new_term(:'ORDERBY', List.wrap(keys_or_function), opts, query)
      end

      def desc(func) when is_function(func) do
        desc(func(func))
      end

      def desc(field) do
        new_term(:'DESC', [field])
      end

      def asc(func) when is_function(func) do
        asc(func(func))
      end

      def asc(field) do
        new_term(:'ASC', [field])
      end

      def skip(n, rql() = query) do
        new_term(:'SKIP', [n], query)
      end

      def limit(n, rql() = query) do
        new_term(:'LIMIT', [n], query)
      end

      # TODO: Test with predicate
      def indexes_of(datum, rql() = query) do
        new_term(:'INDEXES_OF', [datum], query)
      end

      def is_empty?(rql() = query) do
        new_term(:'IS_EMPTY', [], query)
      end

      def union(sequence, rql() = query) do
        new_term(:'UNION', [sequence], query)
      end

      def sample(number, rql() = query) do
        new_term(:'SAMPLE', [number], query)
      end
    end
  end
end

