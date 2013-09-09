defmodule Rethinkdb.Rql.ControlStructures do
  @moduledoc false
  defmacro __using__(_opts) do
    quote do
      def _do(args, expr) do
        new_term(:'FUNCALL', [func(expr), args])
      end

      def for_each(write_query, rql() = query) do
        new_term(:'FOREACH', [func(write_query)], query)
      end

      def branch(test, true_branch, false_branch) do
        new_term(:'BRANCH', [test, true_branch, false_branch])
      end

      def error(message) do
        new_term(:'ERROR', [message])
      end

      def default(value, rql() = query) do
        new_term(:'DEFAULT', [value], query)
      end

      def expr(Range[] = range) do
        make_array(Enum.to_list(range))
      end

      def expr(rql() = query), do: query
      def expr([head|_] = value) when is_tuple(head) do
        expr(HashDict.new(value))
      end

      def expr(value) when is_record(value, HashDict) do
        make_obj(value)
      end

      def expr(values) when is_list(values) do
        make_array(values)
      end

      def expr(value), do: new_term(:EXPR, [value])

      def js(js_string, opts // []) do
        new_term(:'JAVASCRIPT', [js_string], opts)
      end

      def coerce_to(type_name, rql() = query) do
        new_term(:'COERCE_TO', [type_name], query)
      end

      def type_of(rql() = query) do
        new_term(:'TYPEOF', [], query)
      end

      def info(rql() = query) do
        new_term(:'INFO', [], query)
      end

      def json(json_string) do
        new_term(:'JSON', [json_string])
      end
    end
  end
end

