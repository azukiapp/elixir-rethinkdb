defmodule Rethinkdb.Utils.DatumHelpers do
  alias QL2.Datum

  @typep json_term :: :null | boolean | number | binary | Dict.t | [json_term]

  @spec decode(Datum.t) :: json_term
  def decode(Datum[type: :'R_NULL']) do
    nil
  end

  def decode(Datum[type: :'R_BOOL', r_bool: bool]) do
    bool
  end

  def decode(Datum[type: :'R_NUM', r_num: num]) do
    num
  end

  def decode(Datum[type: :'R_STR', r_str: str]) do
    str
  end

  def decode(Datum[type: :'R_ARRAY', r_array: array]) do
    lc item inlist array, do: decode(item)
  end

  def decode(Datum[type: :'R_OBJECT', r_object: object]) do
    HashDict.new(lc Datum.AssocPair[key: key, val: value] inlist object do
      {:'#{key}', decode(value)}
    end)
  end

  @spec encode(json_term) :: Datum.t
  def encode(value) do
    case value do
      null when null == nil or null == :null ->
        Datum.new(type: :'R_NULL')
      bool when is_boolean(bool) ->
        Datum.new(type: :'R_BOOL', r_bool: bool)
      num  when is_number(num) ->
        Datum.new(type: :'R_NUM', r_num: num)
      str  when is_bitstring(str) ->
        Datum.new(type: :'R_STR', r_str: str)
      obj  when is_record(obj, HashDict) ->
        object = lc {key, value} inlist obj.to_list do
          Datum.AssocPair.new(key: "#{key}", val: encode(value))
        end
        Datum.new(type: :'R_OBJECT', r_object: object)
      list when is_list(list) ->
        values = lc item inlist list, do: encode(item)
        Datum.new(type: :'R_ARRAY', r_array: values)
    end
  end
end
