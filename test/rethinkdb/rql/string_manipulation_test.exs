defmodule Rethinkdb.Rql.StringManipulation.Test do
  use Rethinkdb.Case
  use Rethinkdb

  setup_all do
    table = table_to_test("string_manipulation")
    table.insert([
      [name: "Superman" , victories: 200],
      [name: "Batman", victories: 50],
    ]).run!
    {:ok, table: table }
  end

  test "return documents wherein anme match a regular expr", var do
    assert ["Batman"] == var[:table].filter(fn hero ->
      hero[:name].match("^B")
    end)[:name].run!
  end

  test "does parse a string using a regex" do
    assert "mlucy" == r
      .expr("id:0,name:mlucy,foo:bar")
      .match("name:(\\w+)")[:groups][0][:str]
      .run!

    assert nil == r
      .expr("id:0,foo:bar")
      .match("name:(\\w+)")
      .run!
  end
end
