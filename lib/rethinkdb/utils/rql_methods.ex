defmodule Rethinkdb.Utils.RqlMethods do
  alias QL2.Term

  defmacro __using__(_opts) do
    quote do
      import unquote(__MODULE__)
    end
  end

  defmacro rql_method(method) do
    method_def(method, :"#{String.upcase("#{method}")}")
  end

  defmacro rql_method(method, type) do
    method_def(method, type)
  end

  defmacro rql_method(method, type, :without_param) do
    quote do
      def unquote(method)(rql(terms: right)) do
        rql(terms: Term.new(type: unquote(type), args: [right]))
      end
    end
  end

  defmacro rql_method(method, type, :primary) do
    quote do
      def unquote(method)(value) do
        args = [expr(value).terms]
        rql(terms: Term.new(type: unquote(type), args: args))
      end
    end
  end

  defmacro rql_method(method, type, :primary_without) do
    quote do
      def unquote(method)() do
        rql(terms: Term.new(type: unquote(type)))
      end
    end
  end

  defmacro rql_method(method, type, :opts) do
    quote do
      def unquote(method)(value, options // [], rql(terms: left)) do
        opts = lc opt inlist options, do: option_term(opt)
        rql(terms: right) = expr(value)
        rql(terms: Term.new(type: unquote(type), args: [left, right], optargs: opts))
      end
    end
  end

  #defmacro rql_method(method, type, :primary_opts) do
    #quote do
      #def unquote(method)(value, options // []) do
        #args = [expr(value).terms]
        #opts = lc opt inlist options, do: option_term(opt)
        #rql(terms: Term.new(type: unquote(type), args: args, optargs: opts))
      #end
    #end
  #end

  defp method_def(method, type) do
    quote do
      def unquote(method)(value, rql(terms: right)) do
        rql(terms: left) = expr(value)
        rql(terms: Term.new(type: unquote(type), args: [right, left]))
      end
    end
  end
end
