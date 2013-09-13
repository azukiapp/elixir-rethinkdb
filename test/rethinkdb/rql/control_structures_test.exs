defmodule Rethinkdb.Rql.ControlStructures.Test do
  use Rethinkdb.Case
  use Rethinkdb

  setup_all do
    table = table_to_test("control_structures")
    table.insert([
      [superhero: "Superman" , victories: 200],
      [superhero: "Spiderman", victories: 50],
    ]).run!
    {:ok, table: table }
  end

  test "evaluate the expr in the context", var do
    assert "Superman" == r
      ._do(var[:table].filter(superhero: "Superman")[0], fn steel ->
        steel[:superhero]
      end)
      .run!
  end

  test "evaluate a path basead on the expression value", var do

    [spider, steel] = var[:table].order_by(:superhero).map(r.branch(
      r.row[:victories].gt(100),
      r.row[:superhero].add(" is a superhero"),
      r.row[:superhero].add(" is a hero")
    )).run!

    assert Regex.match?(%r/superhero$/, steel)
    assert Regex.match?(%r/hero$/, spider)
  end

  test "loop over a sequence, and write a rql", var do
    result = var[:table].for_each(fn hero ->
      var[:table].get(hero[:id]).update([victories: hero[:victories].add(10)])
    end).run!
    assert 2 == result[:replaced]
    assert [60, 210] == var[:table].order_by(:superhero).map(r.row[:victories]).run!
  end

  test "throw a runtime error" do
    msg  = "impossible code path"
    assert_raise RqlRuntimeError, %r/#{msg}/, fn ->
      r.error(msg).run!
    end
  end

  test "return a default value for a missing value" do
    assert 10 == r.expr(nil).default(10).run!
    assert 20 == r.expr([10])[1].default(20).run!
    assert "foo:1" == r.expr([[exist: 1]]).map(fn p ->
      p[:key].default("foo:").add(p[:exist].coerce_to("string"))
    end)[0].run!
  end

  test :expr do
    assert 1_000 == r.expr(1_000).run!
    assert "bob" == r.expr("bob").run!
    assert true  == r.expr(true ).run!
    assert false == r.expr(false).run!
    assert 3.122 == r.expr(3.122).run!
    assert [1, 2, 3, 4, 5] == r.expr([1, 2, 3, 4, 5]).run!
    assert [1, 2, 3, 4, 5] == r.expr(1..5).run!
  end

  test "expr to hash values" do
    values = [a: 1, b: 2]
    assert HashDict.new(values) == r.expr(HashDict.new(values)).run!
    assert HashDict.new(values) == r.expr(values).run!
  end

  test "expr to expr values" do
    assert r.expr(1).build == r.expr(r.expr(1)).build
  end

  test "execute a js expression" do
    assert "foobar" == r.js("'foo' + 'bar'").run!
    assert_raise RqlRuntimeError, %r/JavaScript.*timed.*1\.300/, fn ->
      r.js("while(true) {}", timeout: 1.3).run!
    end
  end

  test "converts a value of one type into another" do
    assert "10" == r.expr(10).coerce_to("string").run!
    object = HashDict.new(
      name: "Ironman",
      victories: 2001
    )
    assert object == r
      .expr([["name", "Ironman"], ["victories", 2001]])
      .coerce_to("object").run!
  end

  test "get type of a value" do
    assert "STRING" == r.expr("foo").type_of().run!
    assert "NUMBER" == r.expr(10000).type_of().run!
    assert "ARRAY"  == r.expr([1,2]).type_of().run!
  end

  test "get info" do
    result = HashDict.new(type: "NUMBER", value: "1")
    assert result == r.expr(1).info.run!
  end

  test "parse json in server" do
    assert [1, 2, 3] == r.json("[1, 2, 3]").run!
  end
end
