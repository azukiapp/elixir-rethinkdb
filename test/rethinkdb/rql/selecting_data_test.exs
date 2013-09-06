defmodule Rethinkdb.Rql.SelectingData.Test do
  use Rethinkdb.Case, async: false
  use Rethinkdb

  setup_all do
    {conn, table_name} = connect("vertigo", primary_key: "superhero")
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
    table = r.table(table_name)
    table.insert(data, upsert: true).run!(conn)
    {:ok, conn: conn, table: table, table_name: table_name }
  end

  test "select table", var do
    {conn, name} = {var[:conn], var[:table_name]}

    table = r.table(name).info.run!(conn)
    assert name == table[:name]

    table = r.db(conn.db).table(name).info.run!(conn)
    assert name == table[:name]
  end

  test "get a document by primary id", var do
    {conn, table} = {var[:conn], var[:table]}
    result = table.get("Lobo").run!(conn)
    assert "Everything" == result[:superpower]
  end

  test "get all documents where the given matches the value", var do
    {conn, table} = {var[:conn], var[:table]}

    [wolf] = table.getAll("Lobo").run!(conn)
    assert "Lobo" == wolf[:superhero]

    [wolf, const] = table.getAll(["Lobo", "Constantine"]).run!(conn)
    assert "Lobo" == wolf[:superhero]
    assert "Constantine" == const[:superhero]
  end

  test "get all documents with secundary index", var do
    {conn, table} = {var[:conn], var[:table]}
    data  = [superhero: "Lobo", superpower: "Everything"]
    table.index_create("superpower").run(conn)
    table.insert(data, upsert: true).run!(conn)

    [wolf] = table.getAll("Everything", index: :superpower).run!(conn)
    assert "Lobo" == wolf[:superhero]
  end

  test "not implement between" do
    assert_raise Rethinkdb.RqlDriverError, %r/between not implemented yet/, fn ->
      r.table(:any).between(10, 20)
    end
  end

  test "filter by key values", var do
    {conn, table} = {var[:conn], var[:table]}
    result = table.filter(life: 999).run!(conn)
    assert is_list(result)
    assert 2 == length(result)

    result = table.filter(life: 999, superpower: "Everything").run!(conn)
    assert 1 == length(result)
  end

  test "filter by row value", var do
    {conn, table} = {var[:conn], var[:table]}
    [hero] = table.filter(r.row[:life].lt(0)).run!(conn)
    assert "Constantine" == hero[:superhero]
  end

  test "filter by function", var do
    {conn, table} = {var[:conn], var[:table]}
    result = table.filter(fn hero ->
      hero[:abilities].has_fields("super-strength")
    end).run!(conn)
    assert is_list(result)
    assert 2 == length(result)
  end

  test "filter by nested field", var do
    {conn, table} = {var[:conn], var[:table]}
    [hero] = table.filter([abilities: [magic: 100]]).run!(conn)
    assert "Constantine" == hero[:superhero]
  end

  test "filter elements in array", var do
    {conn, table} = {var[:conn], var[:table]}
    filter = r.row[:powers].filter(fn el ->
      el.eq(10)
    end).count().gt(0)
    assert 2 == table.filter(filter).count.run!(conn)
  end
end
