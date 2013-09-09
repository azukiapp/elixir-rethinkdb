defmodule Rethinkdb.Rql.Aggregation do
  @moduledoc false
  defmacro __using__(_opts) do
    quote do
      def reduce(reduction_function, base, rql() = query) do
        new_term(:'REDUCE', [func(reduction_function)], [base: base], query)
      end

      def distinct(rql() = query) do
        new_term(:'DISTINCT', [], query)
      end

      def grouped_map_reduce(grouping, mapping, reduction, base, rql() = query) do
        args = [func(grouping), func(mapping), func(reduction)]
        new_term(:'GROUPED_MAP_REDUCE', args, [base: base], query)
      end

      def group_by(selectors, reduction_object, rql() = query) do
        new_term(:'GROUPBY', [selectors, reduction_object], query)
      end

      def contains(values, rql() = query) do
        new_term(:'CONTAINS', List.wrap(func(values)), query)
      end
    end
  end
end
