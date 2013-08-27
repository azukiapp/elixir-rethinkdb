defmodule Rethinkdb.Ast.Test do
  use Rethinkdb.Case

  alias Rethinkdb.Ast

  test "defined method r" do
    rql = Ast.r.expr(1)
    assert is_record(rql, Ast)
  end

  test "defines a run to call build in connect" do
    Exmeck.mock_run do
      mock.stubs(:_start, fn rql -> rql end)

      conn = mock.module
      rql  = Ast.r.expr(1)

      assert rql == rql.run(conn)
      assert 1   == mock.num_calls(:_start, [rql])
    end
  end

  test "defines a build to generate a ql2 terms" do
    rql  = Ast.r.expr(1)
    term = QL2.Term.new(type: :'DATUM', datum: QL2.Datum.from_value(1))
    assert term == rql.build
  end
end
