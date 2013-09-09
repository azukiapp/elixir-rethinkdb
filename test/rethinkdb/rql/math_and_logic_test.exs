defmodule Rethinkdb.Rql.MathAndLogic.Test do
  use Rethinkdb.Case
  use Rethinkdb

  setup_all do
    {:ok, conn: r.connect}
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
end
