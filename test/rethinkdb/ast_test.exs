defmodule Rethinkdb.Ast.Test do
  use Rethinkdb.Case
  use Rethinkdb.Ast

  test "defined method r" do
    rql = r.expr(1)
    assert is_record(rql, Ast)
  end

  #test "defines a build to generate a ql2 terms" do
    #rql = r.expr(1)
    #assert QL2.Term.new
  #end

  test "defines a run to call build in connect" do
    Exmeck.mock_run do
      mock.stubs(:run, fn rql -> rql end)

      conn = mock.module
      rql  = r.expr(1)

      assert rql == rql.run(conn)
      assert 1   == mock.num_calls(:run, [rql])
    end
  end
end
