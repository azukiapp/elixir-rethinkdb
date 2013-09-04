defmodule Rethinkdb.Rql.SelectingData.Test do
  use Rethinkdb.Case, async: false
  use Rethinkdb

  setup_all do
    {conn, table} = connect("vertigo", primary_key: "superhero")
    {:ok, conn: conn, table: table }
  end

  test "get a document by primary id", var do
    {conn, table} = {var[:conn], var[:table]}
    table  = r.table(table)
    data   = [superhero: "Wolf", superpower: "Everything"]
    table.insert(data, upsert: true).run!(conn)

    result = table.get("Wolf").run!(conn)
    assert data[:superpower] == result[:superpower]
  end

  test "get all documents where the given matches the value", var do
    {conn, table} = {var[:conn], var[:table]}
    table  = r.table(table)
    data   = [
      [superhero: "Wolf", superpower: "Everything"],
      [superhero: "Constantine", superpower: "Indifference"]
    ]
    table.insert(data, upsert: true).run!(conn)

    [wolf] = table.getAll("Wolf").run!(conn)
    assert "Wolf" == wolf[:superhero]

    [wolf, const] = table.getAll(["Wolf", "Constantine"]).run!(conn)
    assert "Wolf" == wolf[:superhero]
    assert "Constantine" == const[:superhero]
  end

  test "get all documents with secundary index", var do
    {conn, table} = {var[:conn], var[:table]}
    table = r.table(table)
    data  = [superhero: "Wolf", superpower: "Everything"]
    table.index_create("superpower").run(conn)
    table.insert(data, upsert: true).run!(conn)

    [wolf] = table.getAll("Everything", index: :superpower).run!(conn)
    assert "Wolf" == wolf[:superhero]
  end
end
