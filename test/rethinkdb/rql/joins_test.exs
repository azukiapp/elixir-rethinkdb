defmodule Rethinkdb.Rql.Joins.Test do
  use Rethinkdb.Case
  use Rethinkdb

  setup_all do
    {conn, [dc, marvel]} = connect(["join_dc", "join_marvel"], primary_key: "superhero")
    data = [
      'join_dc': [
        [superhero: "Superman", strength: 10],
        [superhero: "Batman", strength: 5],
        [superhero: "Shazam", strength: 7, collaborator: "Captain Marvel"],
      ],
      'join_marvel': [
        [superhero: "Spiderman", strength: 8],
        [superhero: "Captain Marvel", strength: 9, origin: "Shazam"],
        [superhero: "Hulk", strength: 11],
      ]
    ]

    lc {table_name, registers} inlist data do
      r.table(table_name).insert(registers).run!(conn)
    end

    {:ok, conn: conn, dc: dc, marvel: marvel}
  end

  test "returns the inner product of two sequences", var do
    {conn, dc, marvel} = {var[:conn], var[:dc], var[:marvel]}
    result = r.table(marvel).inner_join(r.table(dc), fn marvel_row, dc_row ->
      marvel_row[:strength].lt(dc_row[:strength])
    end).run!(conn)

    assert 2 == length(result)
    lc heroes inlist result do
      assert heroes[:left][:strength] < heroes[:right][:strength]
    end
  end

  test "computes a left outer join", var do
    {conn, dc, marvel} = {var[:conn], var[:dc], var[:marvel]}
    result = r.table(marvel).outer_join(r.table(dc), fn marvel_row, dc_row ->
      marvel_row[:strength].lt(dc_row[:strength])
    end).run!(conn)

    assert 3 == length(result)
    lc heroes inlist result do
      assert heroes[:left][:strength] < heroes[:right][:strength]
    end
  end

  test "join that looks up elements in the right table by primary key", var do
    {conn, dc, marvel} = {var[:conn], var[:dc], var[:marvel]}
     query = r.table(dc)
      .has_fields(:collaborator)
      .eq_join(:collaborator, r.table(marvel))

    [heroes] = query.run!(conn)
    assert "Shazam"         == heroes[:left][:superhero]
    assert "Captain Marvel" == heroes[:right][:superhero]
  end

  test "join via eq_join with index", var do
    {conn, dc, marvel} = {var[:conn], var[:dc], var[:marvel]}
     r.table(marvel).index_create(:origin).run(conn)
     query = r.table(dc).eq_join(
       :superhero, r.table(marvel), index: :origin
     )

    [heroes] = query.run!(conn)
    assert "Shazam"         == heroes[:left][:superhero]
    assert "Captain Marvel" == heroes[:right][:superhero]
  end

  test "merge left and right", var do
    {conn, dc, marvel} = {var[:conn], var[:dc], var[:marvel]}
     query = r.table(dc)
      .has_fields(:collaborator)
      .eq_join(:collaborator, r.table(marvel))
      .zip

    [hero] = query.run!(conn)
    assert "Captain Marvel" == hero[:collaborator]
    assert "Shazam" == hero[:origin]
  end
end
