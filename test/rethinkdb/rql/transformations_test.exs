defmodule Rethinkdb.Rql.Transformations.Test do
  use Rethinkdb.Case
  use Rethinkdb

  setup_all do
    table = table_to_test("tf_marvel", primary_key: "superhero")
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

    table.insert(data).run!
    {:ok, table: table }
  end

  test "transform each element of the sequence", var do
    result = var[:table].map(fn hero ->
      hero[:combatPower].add(hero[:compassionPower].mul(2))
    end).run!

    assert Enum.all? result, is_number(&1)
    assert Enum.all? result, &1 > 10
  end

  test "transform each elements with simple predicate", var do
    predicate = r.row[:combatPower].add(r.row[:compassionPower].mul(2))
    result = var[:table].map(predicate).run!
    assert Enum.all? result, is_number(&1)
    assert Enum.all? result, &1 > 10
  end

  test "transform each elements with complex predicate", var do
    predicate = [r.row[:combatPower], r.row[:combatPower]]
    result = var[:table].map(predicate).run!
    assert Enum.all? result, fn [x, y] -> x == y end

    predicate = [cp: r.row[:combatPower], combatPower: r.row["combatPower"]]
    result = var[:table].map(predicate).run!
    assert Enum.all? result, fn obj -> obj[:combatPower] == obj[:cp] end
  end

  test "takes a sequence of objects and a list of fields", var do
    result = var[:table].with_fields([:superhero, :nemesis]).run!
    assert 2 = length(result)
    lc hero inlist result do
      assert hero[:superhero] != nil
      assert hero[:nemesis] != nil
    end
  end

  test "takes a sequence of objects and a lista of fields filtred", var do
    data = HashDict.new(
      nemesis: HashDict.new(evil_organization: ["oscorp"]),
      superhero: "Spiderman"
    )

    query1 = var[:table].with_fields([:superhero], nemesis: [
      evil_organization: true
    ])
    query2 = var[:table].with_fields([
      :superhero, nemesis: :evil_organization
    ])
    query3 = var[:table].with_fields(:superhero, nemesis: :evil_organization)
    assert [data] == query1.run!
    assert [data] == query2.run!
    assert [data] == query3.run!
  end

  test "flattens a sequence of arrays and return a single sequence", var do
    result = var[:table]
      .has_fields(:defeatedMonsters)
      .order_by(:superhero)
      .concat_map(fn hero -> hero[:defeatedMonsters] end)
      .run!
    assert ["wolverine", "rhino"] == result
  end

  test "order documents by fields", var do
    result = var[:table].order_by([:combatPower, :compassionPower]).run!
    assert "Captain Marvel" == Enum.first(result)[:superhero]
    assert "Hulk" == List.last(result)[:superhero]
  end

  test "order documents by index", var do
    var[:table].index_create(:compassionPower).run
    result = var[:table].order_by(:combatPower, index: :compassionPower).run!
    assert "Hulk" == Enum.first(result)[:superhero]
    assert "Spiderman" == List.last(result)[:superhero]
  end

  test "order desc only by index", var do
    var[:table].index_create(:combatPower).run
    result = var[:table].order_by(index: r.desc(:combatPower)).run!
    assert "Hulk" == Enum.first(result)[:superhero]
  end

  test "order desc and asc", var do
    result = var[:table].order_by(
      [r.desc(:combatPower), r.asc(:compassionPower)
    ]).run!

    assert "Hulk" == Enum.first(result)[:superhero]
    assert "Spiderman" == List.last(result)[:superhero]
  end

  test "order by a function", var do
    func = fn doc ->
      doc[:combatPower].sub(doc[:compassionPower].mul(0.2))
    end

    result = var[:table].order_by(func).run!
    assert "Spiderman" == Enum.first(result)[:superhero]
    assert "Hulk" == List.last(result)[:superhero]

    result = var[:table].order_by(r.desc(func)).run!
    assert "Hulk" == Enum.first(result)[:superhero]
    assert "Spiderman" == List.last(result)[:superhero]

    result = var[:table].order_by(r.asc(func)).run!
    assert "Spiderman" == Enum.first(result)[:superhero]
    assert "Hulk" == List.last(result)[:superhero]
  end

  test "skip a number of elements", var do
    [hero] = var[:table].order_by(index: :superhero).skip(2).run!
    assert "Spiderman" == hero[:superhero]
  end

  test "end sequence after given number", var do
    [hero] = var[:table].order_by(index: :superhero).limit(1).run!
    assert "Captain Marvel" == hero[:superhero]
  end

  test "Trim the sequence to within the bounds provided", var do
    [hero] = var[:table].order_by(index: :superhero)[1..2].run!
    assert "Hulk" == hero[:superhero]

    [hero] = var[:table].order_by(index: :superhero)[2..-1].run!
    assert "Spiderman" == hero[:superhero]
  end

  test "get the nth element of a sequence", var do
    assert 2 == r.expr([1, 2, 3])[1].run!
    assert "Captain Marvel" ==
      var[:table].order_by(index: :superhero)[0].run![:superhero]
    assert "Spiderman"      ==
      var[:table].order_by(index: :superhero)[-1].run![:superhero]
  end

  test "get the indexes of an element in a sequence" do
    assert [2] == r.expr(["a", "b", "c"]).indexes_of("c").run!
  end

  test "test if a sequence is empty", var do
    refute var[:table].is_empty?.run!
    assert var[:table].filter(any: true).is_empty?.run!
    assert r.expr([]).is_empty?.run!
  end

  test "concatenate two sequences", var do
    query = var[:table].union([[superhero: "Gambit"]])
    assert 4 == query.count.run!
    assert "Gambit" == query[-1].run![:superhero]
  end

  test "select a random number of documents", var do
    [hero|_] = result = var[:table].sample(2).run!
    assert 2 = length(r.expr([1, 2, 3, 4]).sample(2).run!)
    assert 2 = length(result)
    assert 0 < size(hero[:superhero])
  end
end
