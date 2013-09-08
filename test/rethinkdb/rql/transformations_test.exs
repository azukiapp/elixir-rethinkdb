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
      .order_by(:superhero)
      .concat_map(fn hero -> hero[:defeatedMonsters] end)
      .run!(conn)
    assert ["wolverine", "rhino"] == result
  end

  test "order documents by fields", var do
    {conn, table} = {var[:conn], var[:table]}
    result = table.order_by([:combatPower, :compassionPower]).run!(conn)
    assert "Captain Marvel" == Enum.first(result)[:superhero]
    assert "Hulk" == List.last(result)[:superhero]
  end

  test "order documents by index", var do
    {conn, table} = {var[:conn], var[:table]}
    table.index_create(:compassionPower).run(conn)
    result = table.order_by(:combatPower, index: :compassionPower).run!(conn)
    assert "Hulk" == Enum.first(result)[:superhero]
    assert "Spiderman" == List.last(result)[:superhero]
  end

  test "order desc only by index", var do
    {conn, table} = {var[:conn], var[:table]}
    table.index_create(:combatPower).run(conn)
    result = table.order_by(index: r.desc(:combatPower)).run!(conn)
    assert "Hulk" == Enum.first(result)[:superhero]
  end

  test "order desc and asc", var do
    {conn, table} = {var[:conn], var[:table]}
    result = table.order_by(
      [r.desc(:combatPower), r.asc(:compassionPower)
    ]).run!(conn)

    assert "Hulk" == Enum.first(result)[:superhero]
    assert "Spiderman" == List.last(result)[:superhero]
  end

  test "order by a function", var do
    {conn, table} = {var[:conn], var[:table]}
    func = fn doc ->
      doc[:combatPower].sub(doc[:compassionPower].mul(0.2))
    end

    result = table.order_by(func).run!(conn)
    assert "Spiderman" == Enum.first(result)[:superhero]
    assert "Hulk" == List.last(result)[:superhero]

    result = table.order_by(r.desc(func)).run!(conn)
    assert "Hulk" == Enum.first(result)[:superhero]
    assert "Spiderman" == List.last(result)[:superhero]

    result = table.order_by(r.asc(func)).run!(conn)
    assert "Spiderman" == Enum.first(result)[:superhero]
    assert "Hulk" == List.last(result)[:superhero]
  end

  test "skip a number of elements", var do
    {conn, table} = {var[:conn], var[:table]}
    [hero] = table.order_by(index: :superhero).skip(2).run!(conn)
    assert "Spiderman" == hero[:superhero]
  end

  test "end sequence after given number", var do
    {conn, table} = {var[:conn], var[:table]}
    [hero] = table.order_by(index: :superhero).limit(1).run!(conn)
    assert "Captain Marvel" == hero[:superhero]
  end

  test "Trim the sequence to within the bounds provided", var do
    {conn, table} = {var[:conn], var[:table]}
    [hero] = table.order_by(index: :superhero)[1..2].run!(conn)
    assert "Hulk" == hero[:superhero]

    [hero] = table.order_by(index: :superhero)[2..-1].run!(conn)
    assert "Spiderman" == hero[:superhero]
  end

  test "get the nth element of a sequence", var do
    {conn, table} = {var[:conn], var[:table]}
    assert 2 == r.expr([1, 2, 3])[1].run!(conn)
    assert "Captain Marvel" == table.order_by(index: :superhero)[0].run!(conn)[:superhero]
    assert "Spiderman"      == table.order_by(index: :superhero)[-1].run!(conn)[:superhero]
  end

  test "get the indexes of an element in a sequence", var do
    {conn, _table} = {var[:conn], var[:table]}
    assert [2] == r.expr(["a", "b", "c"]).indexes_of("c").run!(conn)
  end

  test "test if a sequence is empty", var do
    {conn, table} = {var[:conn], var[:table]}
    refute table.is_empty?.run!(conn)
    assert table.filter(any: true).is_empty?.run!(conn)
    assert r.expr([]).is_empty?.run!(conn)
  end

  test "concatenate two sequences", var do
    {conn, table} = {var[:conn], var[:table]}
    query = table.union([[superhero: "Gambit"]])
    assert 4 == query.count.run!(conn)
    assert "Gambit" == query[-1].run!(conn)[:superhero]
  end

  test "select a random number of documents", var do
    {conn, table} = {var[:conn], var[:table]}
    [hero|_] = result = table.sample(2).run!(conn)
    assert 2 = length(r.expr([1, 2, 3, 4]).sample(2).run!(conn))
    assert 2 = length(result)
    assert 0 < size(hero[:superhero])
  end
end
