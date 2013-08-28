defmodule Rethinkdb.DatumHelpers.Test do
  use Rethinkdb.Case

  alias QL2.Datum

  test "parse null value" do
    datum = Datum.new(type: :'R_NULL')
    assert nil == datum.value
  end

  test "create a datum from nil value" do
    assert :'R_NULL' == Datum.from_value(nil).type
    assert :'R_NULL' == Datum.from_value(:null).type
  end

  test "parse bool value" do
    assert true  == Datum.new(type: :'R_BOOL', r_bool: true).value
    assert false == Datum.new(type: :'R_BOOL', r_bool: false).value
  end

  test "create a datum from bool value" do
    assert Datum.new(type: :'R_BOOL', r_bool: true) == Datum.from_value(true)
    assert Datum.new(type: :'R_BOOL', r_bool: false) == Datum.from_value(false)
  end

  test "parse number value" do
    datum = Datum.new(type: :'R_NUM', r_num: 1_000)
    assert 1_000 == datum.value
  end

  test "create a datum from number value" do
    datum = Datum.new(type: :'R_NUM', r_num: 1_000)
    assert datum == Datum.from_value(1_000)
    datum = Datum.new(type: :'R_NUM', r_num: 0.111)
    assert datum == Datum.from_value(0.111)
  end

  test "parse string value" do
    datum = Datum.new(type: :'R_STR', r_str: "Foo Bar")
    assert "Foo Bar" == datum.value
  end

  test "create a datum from string value" do
    datum = Datum.new(type: :'R_STR', r_str: "Foo Bar")
    assert datum == Datum.from_value("Foo Bar")
  end

  test "parse array values" do
    datum = Datum.new(type: :'R_ARRAY', r_array: [])
    assert [] == datum.value

    datum = Datum.new(type: :'R_ARRAY', r_array: [
      Datum.new(type: :'R_STR', r_str: "Foo Bar"),
      Datum.new(type: :'R_NUM', r_num: 1_000)
    ])
    assert ["Foo Bar", 1000] == datum.value
  end

  test "create a array from a array values" do
    datum = Datum.new(type: :'R_ARRAY', r_array: [
      Datum.new(type: :'R_STR', r_str: "Foo Bar"),
      Datum.new(type: :'R_NUM', r_num: 1_000)
    ])
    assert datum == Datum.from_value(["Foo Bar", 1000])
  end

  test "parse object values" do
    value  = Datum.new(type: :'R_NUM', r_num: 1000)
    object = Datum.AssocPair.new(key: "key", val: value)
    datum  = Datum.new(type: :'R_OBJECT', r_object: [object])

    object = HashDict.new(key: 1000)
    assert object == datum.value
  end

  test "create a object from a HashDict value" do
    value  = Datum.new(type: :'R_NUM', r_num: 1000)
    object = Datum.AssocPair.new(key: "key", val: value)
    datum  = Datum.new(type: :'R_OBJECT', r_object: [object])

    object = HashDict.new(key: 1000)
    assert datum == Datum.from_value(object)
  end
end
