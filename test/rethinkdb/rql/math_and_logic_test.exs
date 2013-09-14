defmodule Rethinkdb.Rql.MathAndLogic.Test do
  use Rethinkdb.Case
  use Rethinkdb

  test "logic operators" do
    assert false == r.expr(1).eq(2).run!
    assert true  == r.expr(1).ne(2).run!
    assert false == r.expr(1).gt(2).run!
    assert false == r.expr(1).ge(2).run!
    assert true  == r.expr(1).lt(2).run!
    assert true  == r.expr(1).le(2).run!

    assert true  == r.expr(false)._not.run!
    assert true  == r.expr(true)._and(true).run!
    assert true  == r.expr(false)._or(true).run!
  end

  test "math operators" do
    assert 3 == r.expr(2).add(1).run!
    assert 1 == r.expr(2).add(-1).run!
    assert 1 == r.expr(2).sub(1).run!
    assert 4 == r.expr(2).mul(2).run!
    assert 1 == r.expr(2).div(2).run!
    assert 2 == r.expr(12).mod(10).run!
  end
end
