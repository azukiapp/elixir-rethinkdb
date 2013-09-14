defmodule Rethinkdb.Rql.SelectingData.Test do
  use Rethinkdb.Case, async: false
  use Rethinkdb

  setup_all do
    table = table_to_test(table_name = "vertigo", primary_key: "superhero")
    data = [
      [life: 999, superhero: "Lobo", superpower: "Everything",
        abilities: ['super-strength': 10],
        powers: [10, 20]
      ],
      [life: -10, superhero: "Constantine", superpower: "Indifference",
        abilities: ['magic': 100],
        powers: [20, 30]
      ],
      [life: 999, superhero: "Doctor Manhattan", superpower: "Omnipresence",
        abilities: ['super-strength': 1000],
        powers: [20, 10]
      ],
    ]
    table.insert(data, upsert: true).run!
    {:ok, table: table, table_name: table_name }
  end

  test "select table", var do
    table = r.table(var[:table_name]).info.run!
    assert var[:table_name] == table[:name]
  end

  test "get a document by primary id", var do
    result = var[:table].get("Lobo").run!
    assert "Everything" == result[:superpower]
  end

  test "get all documents where the given matches the value", var do
    [lobo] = var[:table].getAll("Lobo").run!
    assert "Lobo" == lobo[:superhero]

    [lobo, const] = var[:table].getAll(["Lobo", "Constantine"]).run!
    assert "Lobo" == lobo[:superhero]
    assert "Constantine" == const[:superhero]
  end

  test "get all documents with secundary index", var do
    data  = [superhero: "Lobo", superpower: "Everything"]
    var[:table].index_create("superpower").run
    var[:table].insert(data, upsert: true).run!

    [lobo] = var[:table].getAll("Everything", index: :superpower).run!
    assert "Lobo" == lobo[:superhero]
  end

  test "not implement between" do
    assert_raise Rethinkdb.RqlDriverError, %r/between not implemented yet/, fn ->
      r.table(:any).between(10, 20)
    end
  end

  test "filter by key values", var do
    result = var[:table].filter(life: 999).run!
    assert is_list(result)
    assert 2 == length(result)

    result = var[:table].filter(life: 999, superpower: "Everything").run!
    assert 1 == length(result)
  end

  test "filter by row value", var do
    [hero] = var[:table].filter(r.row[:life].lt(0)).run!
    assert "Constantine" == hero[:superhero]
  end

  test "filter by function", var do
    result = var[:table].filter(fn hero ->
      hero[:abilities].has_fields("super-strength")
    end).run!
    assert is_list(result)
    assert 2 == length(result)
  end

  test "filter by nested field", var do
    [hero] = var[:table].filter([abilities: [magic: 100]]).run!
    assert "Constantine" == hero[:superhero]
  end

  test "filter elements in array", var do
    filter = r.row[:powers].filter(fn el ->
      el.eq(10)
    end).count().gt(0)
    assert 2 == var[:table].filter(filter).count.run!
  end
end
