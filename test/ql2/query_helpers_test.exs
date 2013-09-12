defmodule QL2.QueryHelpers.Test do
  use Rethinkdb.Case

  alias QL2.Query
  alias QL2.Term

  test "build a start query with token and database" do
    query = Query.new(
      type: :'START', query: Term.new,
      token: 1, global_optargs: [QL2.global_database("test")]
    )

    assert query == Query.new_start(Term.new, "test", 1)
  end

  test "encode to send in socket" do
    query  = Query.new_start(Term.new, "test", 1)
    encode = query.encode
    binary = [<<iolist_size(encode) :: [size(32), little]>>, encode]
    assert binary == query.encode_to_send
  end
end
