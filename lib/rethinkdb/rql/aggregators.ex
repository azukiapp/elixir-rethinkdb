defmodule Rethinkdb.Rql.Aggregators do
  @moduledoc false
  defmacro __using__(_opts) do
    quote do
      # Aggregators
      def count(filter // nil, rql() = query) do
        if filter, do: query = filter(filter, query)
        new_term(:'COUNT', [], query)
      end

      def count(), do: make_obj('COUNT': nil)
      def sum(attr), do: make_obj('SUM': attr)
      def avg(attr), do: make_obj('AVG': attr)
    end
  end
end

