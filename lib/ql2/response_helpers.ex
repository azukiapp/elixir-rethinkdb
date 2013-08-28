defmodule QL2.ResponseHelpers do
  defmacro __using__(_opts) do
    quote do
      Record.import __MODULE__, as: :record

      @type response :: success | error
      @type success :: {:ok, [any]}
      @type error :: {:error, binary, atom, any}

      @spec value(__MODULE__.t) :: response
      def value(record(type: type, response: [datum])) when
        type in [:'SUCCESS_ATOM', :'SUCCESS_PARTIAL'] do
        {:ok, datum.value}
      end

      def value(record(type: :'SUCCESS_SEQUENCE', response: data)) do
        response = lc datum inlist data, do: datum.value
        {:ok, response}
      end

      def value(record(type: type, response: [errorMsg], backtrace: backtrace)) do
        {:error, type, errorMsg.value, backtrace}
      end
    end
  end
end
