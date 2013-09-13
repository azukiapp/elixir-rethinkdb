defmodule Rethinkdb.Rql.DocumentManipulation.Test do
  use Rethinkdb.Case
  use Rethinkdb

  setup_all do
    table = table_to_test("control_structures")
    table.insert([
      [id: "v1", value: "value 1", powers: 10, spouse: [powers: 10]],
      [id: "v2", value: "value 2", powers: 30, spouse: []],
    ]).run!
    {:ok, table: table}
  end

  test "use row in a array" do
    assert [2, 3] == r.expr([1, 2]).map(r.row.add(1)).run!
  end

  test "filter by row value", var do
    [result] = var[:table].filter(r.row[:powers].gt(20)).run!
    assert "v2" == result[:id]
  end

  test "plucks a one or more attributes", var do
    data = [ k1: "k1", k2: [k3: "k3", k4: "k4"], k5: "k5"]

    result = var[:table].get("v1").pluck(:value).run!
    assert Dict.has_key?(result, :value)
    refute Dict.has_key?(result, :id)

    result = r.expr(data).pluck([:k1, :k5]).run!
    assert Dict.has_key?(result, :k5)
    assert Dict.has_key?(result, :k1)
    refute Dict.has_key?(result, :k2)

    result = r.expr(data).pluck(k2: [k3: true]).run!
    assert Dict.has_key?(result, :k2)
    assert Dict.has_key?(result[:k2], :k3)
    refute Dict.has_key?(result[:k2], :k4)

    assert result == r.expr(data)
      .pluck(k2: [:k3]).run!
  end

  test "not take a attributes", var do
    data = [ k1: "k1", k2: [k3: "k3", k4: "k4"], k5: "k5"]

    result = var[:table].get("v1").without(:value).run!
    refute Dict.has_key?(result, :value)
    assert Dict.has_key?(result, :id)

    result = r.expr(data).without([:k1, k2: [k3: true]]).run!
    refute Dict.has_key?(result, :k1)
    assert Dict.has_key?(result[:k2], :k4)
    refute Dict.has_key?(result[:k2], :k3)
  end

  test "merge a documents", var do
    final = HashDict.new(id: "v3", value: "value 3")
    assert final == r
      .expr([id: "v3"])
      .merge([value: "value 3"]).run!

    result = var[:table].get("v1").merge(
      var[:table].filter(id: "v2").with_fields([:value])[0]
    ).run!

    assert "v1" == result[:id]
    assert "value 2" == result[:value]
  end

  test "replace a nested object in merge" do
    result = r.expr(weapons: [
      'spectacular graviton beam': [
        dmg: 10, cooldown: 20
      ]
    ]).merge(weapons: r.literal(
      'repulsor rays': [
        dmg: 3, cooldown: 0
      ]
    )).run!

    assert Dict.has_key?(result[:weapons], :'repulsor rays')
    refute Dict.has_key?(result[:weapons], :'spectacular graviton beam')
  end

  test "use literal to remove keys from a object" do
    key = :'spectacular graviton beam'
    result = r.expr(weapons: [
      [{key, dmg: 10, cooldown: 20}]
    ]).merge(weapons:
      [{key, r.literal()}]
    ).run!

    assert Dict.has_key?(result[:weapons], key)
    assert nil == result[:weapons][key]
  end

  test "append and prepend" do
    array = [1, 2, 3, 4]
    assert array ++ [5] == r.expr(array).append(5).run!
    assert [0 | array]  == r.expr(array).prepend(0).run!
  end

  test "remove the elements of one array from another array" do
    assert [1] == r.expr([1, 2, 3]).difference([2, 3]).run!
  end

  test "insert a value in array if not exist" do
    assert [1, 2, 3] == r.expr([1, 2, 3]).set_insert(3).run!
    assert [1, 2, 3, 4] == r.expr([1, 2, 3]).set_insert(4).run!
  end

  test "return a intersect two arrays" do
    assert [2, 4] == r.expr([1, 2, 3, 4]).set_intersection([2, 5, 4]).run!
  end

  test "set_difference" do
    assert [1, 3] == r.expr([1, 2, 3, 4]).set_difference([2, 5, 4]).run!
  end

  test "get a simple field from an object", var do
    assert "value 1" == var[:table].order_by(:id)[0][:value].run!
  end

  test "test a object has all of the specified fileds", var do
    assert 2  == var[:table].has_fields(:value).count.run!
    assert 2  == var[:table].has_fields([:value, :id]).count.run!
    assert [] == var[:table].has_fields(:any).run!
    assert var[:table][0].has_fields(:value).run!
  end

  test "test a object has field in nested attributes", var do
    assert 1 == var[:table].has_fields(spouse: [powers: true]).count.run!
    assert 1 == var[:table].has_fields(spouse: :powers).count.run!
  end

  test "manipulating arrays of elements by index" do
    assert [1, 2, 3]    == r.expr([1, 3]).insert_at(1, 2).run!
    assert [1, 2, 3, 4] == r.expr([1, 4]).splice_at(1, [2, 3]).run!
    assert [1, 3]       == r.expr([1, 2, 3]).delete_at(1).run!
    assert [1, 4]       == r.expr([1, 2, 3, 4]).delete_at(1,3).run!
    assert [1, 2, 3]    == r.expr([1, 3, 3]).change_at(1, 2).run!
  end

  test "get an array containing all of the objetc's keys", var do
    assert ["id", "powers", "spouse", "value"] == var[:table][0].keys.run!
  end
end
