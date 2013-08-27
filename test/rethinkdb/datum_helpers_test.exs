defmodule Rethinkdb.DatumHelpers.Test do
  use Rethinkdb.Case

  alias QL2.Datum
  #alias Rethinkdb.DatumHelpers, as: Helpers
  import Rethinkdb.DatumHelpers

  test "parse null value" do
    datum = Datum.new(type: :'R_NULL')
    assert nil == decode(datum)
  end

  test "parse bool value" do
    assert true  == decode(Datum.new(type: :'R_BOOL', r_bool: true))
    assert false == decode(Datum.new(type: :'R_BOOL', r_bool: false))
  end

  test "parse number value" do
    datum = Datum.new(type: :'R_NUM', r_num: 1_000)
    assert 1_000 == decode(datum)
  end

  test "parse string value" do
    datum = Datum.new(type: :'R_STR', r_str: "Foo Bar")
    assert "Foo Bar" == decode(datum)
  end

  test "parse array values" do
    datum = Datum.new(type: :'R_ARRAY', r_array: [])
    assert [] == decode(datum)

    datum = Datum.new(type: :'R_ARRAY', r_array: [
      Datum.new(type: :'R_STR', r_str: "Foo Bar"),
      Datum.new(type: :'R_NUM', r_num: 1_000)
    ])
    assert ["Foo Bar", 1000] == decode(datum)
  end

  test "parse object values" do
    value  = Datum.new(type: :'R_NUM', r_num: 1000)
    object = Datum.AssocPair.new(key: "key", val: value)
    datum  = Datum.new(type: :'R_OBJECT', r_object: [object])

    object = HashDict.new(key: 1000)
    assert object == decode(datum)
  end
end
