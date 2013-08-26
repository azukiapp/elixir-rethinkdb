defmodule Rethinkdb.DatumHelpers do
  alias QL2.Datum

  def decode(Datum[type: :'R_NULL']) do
    :null
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
end
