defmodule Rethinkdb.Rql.ControlStructures.Test do
  use Rethinkdb.Case
  use Rethinkdb

  setup_all do
    {conn, table_name} = connect("control_structures")
    table = r.table(table_name)
    table.insert([
      [superhero: "Superman" , victories: 200],
      [superhero: "Spiderman", victories: 50],
    ]).run!(conn)
    {:ok, conn: conn, table: table }
  end

  test "evaluate the expr in the context", var do
    {conn, table} = {var[:conn], var[:table]}
    assert "Superman" == r
      ._do(table.filter(superhero: "Superman")[0], fn steel ->
        steel[:superhero]
      end)
      .run!(conn)
  end

  test "evaluate a path basead on the expression value", var do
    {conn, table} = {var[:conn], var[:table]}

    [spider, steel] = table.order_by(:superhero).map(r.branch(
      r.row[:victories].gt(100),
      r.row[:superhero].add(" is a superhero"),
      r.row[:superhero].add(" is a hero")
    )).run!(conn)

    assert Regex.match?(%r/superhero$/, steel)
    assert Regex.match?(%r/hero$/, spider)
  end

  test "loop over a sequence, and write a rql", var do
    {conn, table} = {var[:conn], var[:table]}
    result = table.for_each(fn hero ->
      table.get(hero[:id]).update([victories: hero[:victories].add(10)])
    end).run!(conn)
    assert 2 == result[:replaced]
    assert [60, 210] == table.order_by(:superhero).map(r.row[:victories]).run!(conn)
  end

  test "throw a runtime error", var do
    conn = var[:conn]
    msg  = "impossible code path"
    assert_raise RqlRuntimeError, %r/#{msg}/, fn ->
      r.error(msg).run!(conn)
    end
  end

  test "return a default value for a missing value", var do
    conn = var[:conn]
    assert 10 == r.expr(nil).default(10).run!(conn)
    assert 20 == r.expr([10])[1].default(20).run!(conn)
    assert "foo:1" == r.expr([[exist: 1]]).map(fn p ->
      p[:key].default("foo:").add(p[:exist].coerce_to("string"))
    end)[0].run!(conn)
  end

  test :expr, var do
    conn = var[:conn]
    assert 1_000 == r.expr(1_000).run!(conn)
    assert "bob" == r.expr("bob").run!(conn)
    assert true  == r.expr(true ).run!(conn)
    assert false == r.expr(false).run!(conn)
    assert 3.122 == r.expr(3.122).run!(conn)
    assert [1, 2, 3, 4, 5] == r.expr([1, 2, 3, 4, 5]).run!(conn)
    assert [1, 2, 3, 4, 5] == r.expr(1..5).run!(conn)
  end

  test "expr to hash values", var do
    conn   = var[:conn]
    values = [a: 1, b: 2]
    assert HashDict.new(values) == r.expr(HashDict.new(values)).run!(conn)
    assert HashDict.new(values) == r.expr(values).run!(conn)
  end

  test "expr to expr values" do
    assert r.expr(1).build == r.expr(r.expr(1)).build
  end

  test "logic operators", var do
    conn = var[:conn]
    assert false == r.expr(1).eq(2).run!(conn)
    assert true  == r.expr(1).ne(2).run!(conn)
    assert false == r.expr(1).gt(2).run!(conn)
    assert false == r.expr(1).ge(2).run!(conn)
    assert true  == r.expr(1).lt(2).run!(conn)
    assert true  == r.expr(1).le(2).run!(conn)

    assert true  == r.expr(false)._not.run!(conn)
    assert true  == r.expr(true)._and(true).run!(conn)
    assert true  == r.expr(false)._or(true).run!(conn)
  end

  test "math operators", var do
    conn = var[:conn]
    assert 3 == r.expr(2).add(1).run!(conn)
    assert 1 == r.expr(2).add(-1).run!(conn)
    assert 1 == r.expr(2).sub(1).run!(conn)
    assert 4 == r.expr(2).mul(2).run!(conn)
    assert 1 == r.expr(2).div(2).run!(conn)
    assert 2 == r.expr(12).mod(10).run!(conn)
  end

  test "execute a js expression", var do
    conn = var[:conn]
    assert "foobar" == r.js("'foo' + 'bar'").run!(conn)
    assert_raise RqlRuntimeError, %r/JavaScript.*timed.*1\.300/, fn ->
      r.js("while(true) {}", timeout: 1.3).run!(conn)
    end
  end

  test "converts a value of one type into another", var do
    conn = var[:conn]
    assert "10" == r.expr(10).coerce_to("string").run!(conn)
    object = HashDict.new(
      name: "Ironman",
      victories: 2001
    )
    assert object == r
      .expr([["name", "Ironman"], ["victories", 2001]])
      .coerce_to("object").run!(conn)
  end

  test "get type of a value", var do
    conn = var[:conn]
    assert "STRING" == r.expr("foo").type_of().run!(conn)
    assert "NUMBER" == r.expr(10000).type_of().run!(conn)
    assert "ARRAY"  == r.expr([1,2]).type_of().run!(conn)
  end

  test "get info", var do
    conn = var[:conn]
    result = HashDict.new(type: "NUMBER", value: "1")
    assert result == r.expr(1).info.run!(conn)
  end

  test "parse json in server", var do
    conn = var[:conn]
    assert [1, 2, 3] == r.json("[1, 2, 3]").run!(conn)
  end
end
