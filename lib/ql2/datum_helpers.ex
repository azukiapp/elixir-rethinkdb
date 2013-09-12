defmodule QL2.DatumHelpers do
  alias Rethinkdb.Rql

  defmacro __using__(_opts) do
    quote do
      Record.import __MODULE__, as: :datum

      @typep json_term :: :null | boolean | number | binary | Dict.t | [json_term]
      @typep t :: __MODULE__

      @spec value(t) :: json_term
      def value(datum(type: :'R_NULL')) do
        nil
      end

      def value(datum(type: :'R_BOOL', r_bool: bool)) do
        bool
      end

      def value(datum(type: :'R_NUM', r_num: num)) do
        num
      end

      def value(datum(type: :'R_STR', r_str: str)) do
        str
      end

      def value(datum(type: :'R_ARRAY', r_array: array)) do
        lc item inlist array, do: value(item)
      end

      def value(datum(type: :'R_OBJECT', r_object: object)) do
        HashDict.new(lc QL2.Datum.AssocPair[key: key, val: value] inlist object do
          {:'#{key}', value(value)}
        end)
      end

      @spec from_value(json_term) :: t
      def from_value(value) do
        case value do
          null when null == nil or null == :null ->
            new(type: :'R_NULL')
          bool when is_boolean(bool) ->
            new(type: :'R_BOOL', r_bool: bool)
          num  when is_number(num) ->
            new(type: :'R_NUM', r_num: num)
          str  when is_bitstring(str) ->
            new(type: :'R_STR', r_str: str)
          atom when is_atom(atom) ->
            new(type: :'R_STR', r_str: "#{atom}")
          rql when is_record(rql, Rql) ->
            Rql.build(rql)
          obj  when is_record(obj, HashDict) ->
            object = lc {key, value} inlist obj.to_list do
              QL2.Datum.AssocPair.new(key: "#{key}", val: from_value(value))
            end
            new(type: :'R_OBJECT', r_object: object)
          [{_, _} | _] = obj ->
            object = lc {key, value} inlist obj do
              QL2.Datum.AssocPair.new(key: "#{key}", val: from_value(value))
            end
            new(type: :'R_OBJECT', r_object: object)
          list when is_list(list) ->
            values = lc item inlist list, do: from_value(item)
            new(type: :'R_ARRAY', r_array: values)
        end
      end
    end
  end
end
