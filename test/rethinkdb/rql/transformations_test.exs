defmodule Rethinkdb.Rql.Transformations.Test do
  use Rethinkdb.Case
  use Rethinkdb

  setup_all do
    {conn, table_name} = connect("tf_marvel", primary_key: "superhero")
    data = [
      [superhero: "Spiderman", combatPower: 9, compassionPower: 10,
        nemesis: [evil_organization: ["oscorp"]],
        defeatedMonsters: ["rhino"]
      ],
      [superhero: "Captain Marvel", combatPower: 9, compassionPower: 8,
        nemesis: []
      ],
      [superhero: "Hulk", combatPower: 11, compassionPower: 5,
        defeatedMonsters: ["wolverine"]
      ],
    ]

    table = r.table(table_name)
    table.insert(data).run!(conn)
    {:ok, conn: conn, table: table }
  end

  test "transform each element of the sequence", var do
    {conn, table} = {var[:conn], var[:table]}
    result = table.map(fn hero ->
      hero[:combatPower].add(hero[:compassionPower].mul(2))
    end).run!(conn)

    assert Enum.all? result, is_number(&1)
    assert Enum.all? result, &1 > 10
  end

  test "takes a sequence of objects and a list of fields", var do
    {conn, table} = {var[:conn], var[:table]}
    result = table.with_fields([:superhero, :nemesis]).run!(conn)
    assert 2 = length(result)
    lc hero inlist result do
      assert hero[:superhero] != nil
      assert hero[:nemesis] != nil
    end
  end

  test "takes a sequence of objects and a lista of fields filtred", var do
    {conn, table} = {var[:conn], var[:table]}
    data = HashDict.new(
      nemesis: HashDict.new(evil_organization: ["oscorp"]),
      superhero: "Spiderman"
    )

    query1 = table.with_fields([:superhero], nemesis: [
      evil_organization: true
    ])
    query2 = table.with_fields([
      :superhero, nemesis: :evil_organization
    ])
    query3 = table.with_fields(:superhero, nemesis: :evil_organization)
    assert [data] == query1.run!(conn)
    assert [data] == query2.run!(conn)
    assert [data] == query3.run!(conn)
  end

  test "flattens a sequence of arrays and return a single sequence", var do
    {conn, table} = {var[:conn], var[:table]}
    result = table
      .has_fields(:defeatedMonsters)
      .concat_map(fn hero -> hero[:defeatedMonsters] end)
      .run!(conn)
    assert ["rhino", "wolverine"] == result
  end

  test "oder by documents", var do
    {conn, table} = {var[:conn], var[:table]}
    result = table.order_by([:combatPower, :compassionPower]).run!(conn)
    assert "Captain Marvel" == Enum.first(result)[:superhero]
    assert "Hulk" == List.last(result)[:superhero]
  end
end
