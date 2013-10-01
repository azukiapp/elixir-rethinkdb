defmodule Rethinkdb.Rql.TermsConstructor do
  @moduledoc false

  defmacro __using__(_opts) do
    quote do
      defrecordp :term, type: nil, args: [], optargs: []

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

      defp new_term(type, args, opts) when is_list(opts) or is_record(opts, HashDict) do
        new_term(type, args, opts, rql())
      end

      defp new_term(type, args, optargs, rql(terms: terms)) do
        rql(terms: terms ++ [term(type: type, args: args, optargs: optargs)])
      end

      defp make_array(items) when is_list(items) do
        new_term(:'MAKE_ARRAY', items)
      end

      defp make_obj(values) do
        new_term(:'MAKE_OBJ', [], values)
      end

      defp var(n) do
        new_term(:'VAR', [n])
      end

      # Function helpers
      defp func(func) when is_function(func) do
        {_, arity} = :erlang.fun_info(func, :arity)
        arg_count  = :lists.seq(1, arity)
        func_args  = lc n inlist arg_count, do: var(n)

        args = case apply(func, func_args) do
          [{key, _}|_] = obj when key != __MODULE__ -> [make_obj(obj)]
          array when is_list(array) -> [make_array(array)]
          rql() = query -> [query]
        end

        new_term(:'FUNC', [expr(arg_count) | args])
      end

      defp func(value), do: value
    end
  end
end

