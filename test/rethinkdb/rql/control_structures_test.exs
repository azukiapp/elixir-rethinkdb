defmodule Rethinkdb.Rql.ControlStructures.Test do
  use Rethinkdb.Case
  use Rethinkdb

  setup_all do
    {:ok, conn: r.connect(db: "test")}
  end

  test :expr, var do
    assert 1_000 == r.expr(1_000).run!(var[:conn])
    assert "bob" == r.expr("bob").run!(var[:conn])
    assert true  == r.expr(true ).run!(var[:conn])
    assert false == r.expr(false).run!(var[:conn])
    assert 3.122 == r.expr(3.122).run!(var[:conn])
    assert [1, 2, 3, 4, 5] == r.expr([1, 2, 3, 4, 5]).run!(var[:conn])
    assert [1, 2, 3, 4, 5] == r.expr(1..5).run!(var[:conn])
  end

  test "expr to hash values", var do
    values = [a: 1, b: 2]
    assert HashDict.new(values) == r.expr(HashDict.new(values)).run!(var[:conn])
    assert HashDict.new(values) == r.expr(values).run!(var[:conn])
  end

  test "expr to expr values" do
    assert r.expr(1).build == r.expr(r.expr(1)).build
  end

  test "logic operators", var do
    assert false == r.expr(1).eq(2).run!(var[:conn])
    assert true  == r.expr(1).ne(2).run!(var[:conn])
    assert false == r.expr(1).gt(2).run!(var[:conn])
    assert false == r.expr(1).ge(2).run!(var[:conn])
    assert true  == r.expr(1).lt(2).run!(var[:conn])
    assert true  == r.expr(1).le(2).run!(var[:conn])

    assert true  == r.expr(false)._not.run!(var[:conn])
    assert true  == r.expr(true)._and(true).run!(var[:conn])
    assert true  == r.expr(false)._or(true).run!(var[:conn])
  end

  test "math operators", var do
    assert 3 == r.expr(2).add(1).run!(var[:conn])
    assert 1 == r.expr(2).add(-1).run!(var[:conn])
    assert 1 == r.expr(2).sub(1).run!(var[:conn])
    assert 4 == r.expr(2).mul(2).run!(var[:conn])
    assert 1 == r.expr(2).div(2).run!(var[:conn])
    assert 2 == r.expr(12).mod(10).run!(var[:conn])
  end

  test "define append and prepend", var do
    array = [1, 2, 3, 4]
    assert array ++ [5] == r.expr(array).append(5).run!(var[:conn])
    assert [0 | array]  == r.expr(array).prepend(0).run!(var[:conn])
  end

  test "get info", var do
    result = HashDict.new(type: "NUMBER", value: "1")
    assert result == r.expr(1).info.run!(var[:conn])
  end
end
