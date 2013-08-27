defmodule Rethinkdb.Ast do
  alias QL2.Datum
  alias QL2.Term

  @typep datum_arg :: :null | boolean | number | binary
  @typep expr_arg :: Dict.t | {any, any} | [expr_arg] | fun | atom | Term.t | Term.AssocPair.t | datum_arg

  defrecordp :rql, __MODULE__, terms: []

  # TODO: Adding support initial expr
  def r, do: __MODULE__

  # ACCESSING RQL
  def run(conn, rql() = query) do
    conn._start(query)
  end

  # CONTROL STRUCTURES
  @spec expr(expr_arg) :: :rql.t
  def expr(value) do
    rql(terms: Term.new(type: :'DATUM', datum: Datum.from_value(value)))
  end

  # Utils
  @spec build(:rql.t) :: Term.t
  def build(rql(terms: terms)) do
    terms
  end
end
