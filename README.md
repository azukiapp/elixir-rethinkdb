# Elixir Rethinkdb Driver

This is a prototype of a [RethinkDB](http://www.rethinkdb.com) client driver written in Elixir.
This driver utilizes Erlang R16B01 and Elixir 0.10.1.

Current version was tested: RethinkDB 1.8.1

## Installation

Add the following to your list of dependencies in mix.exs:

```elixir
{ :rethinkdb, github: "azukiapp/elixir-rethinkdb" }
```

## Example

```elixir
defmodule Simple.Marvel do
  use Rethinkdb

  def get(name) do
    r.table("marvel").get(name).run!
  end
end

defmodule Simple.App do
  use Rethinkdb

  def start do
    r.connect("rethinkdb://localhost:28015/test").repl
  end
end
```

## Usage in iex

Open connection and create a table:

```elixir
iex> Rethinkdb.start
iex> use Rethinkdb
iex> conn = rr.connect(db: test)
iex> rr.table_create("marvel", primary_key: "name").run(conn)
{:ok, #HashDict<[created: 1.0]>}
```

Insert a document
```elixir
iex> batman = [name: "batman", rich: true, cars: [1, 2, 3]]
iex> rr.table("marvel").insert(batman).run!(conn)
#HashDict<[deleted: 0.0, errors: 0.0, inserted: 1.0, replaced: 0.0,
  skipped: 0.0, unchanged: 0.0]>
```

Making life easier:
```elixir
iex> conn.repl # now this connection is default
iex> table = rr.table("marvel")
```

Update document:
```elixir
iex> table.get("batman").update(age: 30).run!
iex> table.get("batman").run!
#HashDict<[age: 30.0, cars: [1.0, 2.0, 3.0], name: "batman", rich: true]>
iex> table[0]["name"].run!
"batman"
```

Map a document with function:
```elixir
iex> table.map(fn hero ->
  hero[:name].add(" ").add(hero[:age].coerce_to("string"))
end).run!
["batman 30"]
```

## Overview

The Elixir driver is most similar to the [official Python driver](http://www.rethinkdb.com/api/#py).
Most of the functions have the same names as in the python driver.

## Differences from official RethinkDB drivers

* Due to a compatibility problem with the function of `r` iex, to run `use` Rethinkdb in iex,
`rr` method is imported in place of the method `r` as is usual.

## License

"Azuki" and the Azuki logo are copyright (c) 2013 Azuki Serviços de Internet LTDA..

Exdocker source code is released under Apache 2 License.

Check LEGAL and LICENSE files for more information.


[![Bitdeli Badge](https://d2weczhvl823v0.cloudfront.net/azukiapp/elixir-rethinkdb/trend.png)](https://bitdeli.com/free "Bitdeli Badge")

