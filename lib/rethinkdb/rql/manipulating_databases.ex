defmodule Rethinkdb.Rql.ManipulatingDatabases do
  @moduledoc false

  defmacro __using__(_opts) do
    quote do
      # MANIPULATING DATABASES
      @doc """
      Create a database. A RethinkDB database is a collection of
      tables, similar to relational databases.

      If successful, the operation returns an object: HashDict#<[created: 1]>.
      If a database with the same name already exists the operation throws RqlRuntimeError.

      Example: Create a database named 'superheroes'.

          iex> r.db_create("superheroes").run!(conn)
          HashDict#<[created: 1]>
      """
      @spec db_create(name) :: t
      def db_create(name), do: new_term(:'DB_CREATE', [name])

      @doc """
      Drop a database. The database, all its tables, and
      corresponding data will be deleted.

      If successful, the operation returns the object HashDict#<[dropped: 1]>.
      If the specified database doesn't exist a RqlRuntimeError is thrown.

      Example: Drop a database named 'superheroes'.

          iex> r.db_drop("superheroes").run!(conn)
          HashDict#<[dropped: 1]>
      """
      @spec db_drop(name) :: t
      def db_drop(name), do: new_term(:'DB_DROP', [name])

      @doc """
      List all database names in the system.

      The result is a list of strings.

      Example: List all databases.

          iex> r.db_list().run!(conn)
          ["test", "rethinkdb_test"]
      """
      @spec db_list :: t
      def db_list, do: new_term(:'DB_LIST')
    end
  end
end
