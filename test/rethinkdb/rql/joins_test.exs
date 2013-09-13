defmodule Rethinkdb.Rql.Joins.Test do
  use Rethinkdb.Case
  use Rethinkdb

  setup_all do
    [tb_dc, tb_marvel] = table_to_test(["join_dc", "join_marvel"], primary_key: "superhero")
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
      r.table(table_name).insert(registers).run!
    end

    {:ok, dc: tb_dc, marvel: tb_marvel}
  end

  test "returns the inner product of two sequences", var do
    {tb_dc, tb_marvel} = {var[:dc], var[:marvel]}
    result = tb_marvel.inner_join(tb_dc, fn marvel_row, dc_row ->
      marvel_row[:strength].lt(dc_row[:strength])
    end).run!

    assert 2 == length(result)
    lc heroes inlist result do
      assert heroes[:left][:strength] < heroes[:right][:strength]
    end
  end

  test "computes a left outer join", var do
    {tb_dc, tb_marvel} = {var[:dc], var[:marvel]}
    result = tb_marvel.outer_join(tb_dc, fn marvel_row, dc_row ->
      marvel_row[:strength].lt(dc_row[:strength])
    end).run!

    assert 3 == length(result)
    lc heroes inlist result do
      assert heroes[:left][:strength] < heroes[:right][:strength]
    end
  end

  test "join that looks up elements in the right table by primary key", var do
    {tb_dc, tb_marvel} = {var[:dc], var[:marvel]}
     query = tb_dc
      .has_fields(:collaborator)
      .eq_join(:collaborator, tb_marvel)

    [heroes] = query.run!
    assert "Shazam"         == heroes[:left][:superhero]
    assert "Captain Marvel" == heroes[:right][:superhero]
  end

  test "join via eq_join with index", var do
    {tb_dc, tb_marvel} = {var[:dc], var[:marvel]}
     tb_marvel.index_create(:origin).run
     query = tb_dc.eq_join(
       :superhero, tb_marvel, index: :origin
     )

    [heroes] = query.run!
    assert "Shazam"         == heroes[:left][:superhero]
    assert "Captain Marvel" == heroes[:right][:superhero]
  end

  test "merge left and right", var do
    {tb_dc, tb_marvel} = {var[:dc], var[:marvel]}
     query = tb_dc
      .has_fields(:collaborator)
      .eq_join(:collaborator, tb_marvel)
      .zip

    [hero] = query.run!
    assert "Captain Marvel" == hero[:collaborator]
    assert "Shazam" == hero[:origin]
  end
end
