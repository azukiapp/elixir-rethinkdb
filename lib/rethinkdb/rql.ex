defmodule Rethinkdb.Rql do
  alias QL2.Datum
  alias QL2.Term
  alias Rethinkdb.Utils
  alias Rethinkdb.RqlDriverError

  defrecordp :term, type: nil, args: [], optargs: []
  defrecordp :rql, __MODULE__, terms: []

  @type conn   :: Rethinkdb.Connection.t
  @type url    :: binary
  @type params :: Keyword.t
  @type term   :: Term.t

  @typep datum_arg :: :null | boolean | number | binary
  @typep expr_arg :: Dict.t | {any, any} | [expr_arg] | fun | atom | term | Term.AssocPair.t | datum_arg

  # TODO: Adding support initial expr
  @spec r :: atom
  def r, do: __MODULE__

  @doc false
  def rr, do: r

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
  def db_drop(name), do: new_term(:'DB_DROP', [name])

  @doc """
  List all database names in the system.

  The result is a list of strings.

  Example: List all databases.

      iex> r.db_list().run!(conn)
      ["test", "rethinkdb_test"]
  """
  def db_list, do: new_term(:'DB_LIST')

  # MANIPULATING TABLES
  # TODO: Test options
  def table_create(name, rql() = query) do
    table_create(name, [], query)
  end

  def table_create(name, opts // [], rql() = query // rql()) do
    new_term(:'TABLE_CREATE', [name], opts, query)
  end

  def table_drop(name, rql() = query // rql()) do
    new_term(:'TABLE_DROP', [name], [], query)
  end

  def table_list(rql() = query // rql()) do
    new_term(:'TABLE_LIST', [], query)
  end

  def index_create(index, rql() = query) do
    new_term(:'INDEX_CREATE', [index], query)
  end

  def index_create(index, func, rql() = query) do
    new_term(:'INDEX_CREATE', [index, func(func)], query)
  end

  def index_drop(index, rql() = query) do
    new_term(:'INDEX_DROP', [index], query)
  end

  def index_list(rql() = query) do
    new_term(:'INDEX_LIST', [], query)
  end

  # Write Data
  def insert(data, opts // [], rql() = query) do
    new_term(:'INSERT', [data], opts, query)
  end

  def update(data, rql() = query) do
    update(data, [], query)
  end

  def update(func, opts, rql() = query) when is_function(func) do
    update(func(func), opts, query)
  end

  def update(data, opts, rql() = query) do
    new_term(:'UPDATE', [data], opts, query)
  end

  def replace(data, rql() = query) do
    replace(data, [], query)
  end

  def replace(func, opts, rql() = query) when is_function(func) do
    replace(func(func), opts, query)
  end

  def replace(data, opts, rql() = query) do
    new_term(:'REPLACE', [data], opts, query)
  end

  def delete(opts // [], rql() = query) do
    new_term(:'DELETE', [], opts, query)
  end

  # Selecting data
  def db(name) do
    new_term(:'DB', [name])
  end

  def table(name, rql() = query // rql()) do
    new_term(:'TABLE', [name], query)
  end

  def get(id, rql() = query) do
    new_term(:'GET', [id], [], query)
  end

  def getAll(ids, rql() = query) do
    getAll(ids, [], query)
  end

  def getAll(ids, opts, rql() = query) when not is_list(ids) do
    getAll([ids], opts, query)
  end

  def getAll(ids, opts, rql() = query) do
    new_term(:'GET_ALL', ids, opts, query)
  end

  def between(_left_bound, _right_bound, rql() = _query) do
    RqlDriverError.not_implemented(:between)
  end

  def filter(rql() = predicate, rql() = query) do
    filter(fn _ -> predicate end, query)
  end

  def filter(func, rql() = query) when is_function(func) do
    new_term(:'FILTER', [func(func)], query)
  end

  def filter(predicate, rql() = query) do
    new_term(:'FILTER', [predicate], query)
  end

  # Joins
  def inner_join(sequence, predicate, rql() = query) do
    new_term(:'INNER_JOIN', [sequence, func(predicate)], query)
  end

  def outer_join(sequence, predicate, rql() = query) do
    new_term(:'OUTER_JOIN', [sequence, func(predicate)], query)
  end

  def eq_join(left_attr, other_table, rql() = query) do
    eq_join(left_attr, other_table, [], query)
  end

  def eq_join(left_attr, other_table, opts, rql() = query) do
    new_term(:'EQ_JOIN', [left_attr, other_table], opts, query)
  end

  def zip(rql() = query) do
    new_term(:'ZIP', [], query)
  end

  # Transformations
  def map(func, rql() = query) do
    new_term(:'MAP', [func(func)], query)
  end

  def with_fields(fields, options // [], rql() = query) do
    new_term(:'WITH_FIELDS', [fields, options], query)
  end

  def concat_map(func, rql() = query) do
    new_term(:'CONCATMAP', [func(func)], query)
  end

  def order_by([{:index, _}|_] = opts, rql() = query) do
    order_by([], opts, query)
  end

  def order_by(keys_or_function, opts // [], rql() = query) do
    if is_function(keys_or_function) do
      keys_or_function = func(keys_or_function)
    end
    new_term(:'ORDERBY', List.wrap(keys_or_function), opts, query)
  end

  def desc(func) when is_function(func) do
    desc(func(func))
  end

  def desc(field) do
    new_term(:'DESC', [field])
  end

  def asc(func) when is_function(func) do
    asc(func(func))
  end

  def asc(field) do
    new_term(:'ASC', [field])
  end

  def skip(n, rql() = query) do
    new_term(:'SKIP', [n], query)
  end

  def limit(n, rql() = query) do
    new_term(:'LIMIT', [n], query)
  end

  # Aggregation
  def count(filter // nil, rql() = query) do
    if filter, do: query = filter(filter, query)
    new_term(:'COUNT', [], query)
  end

  # Accessing Rql
  def run(conn, rql() = query) do
    Utils.RunQuery.run(build(query), conn)
  end

  def run!(conn, rql() = query) do
    Utils.RunQuery.run!(build(query), conn)
  end

  # Math And Logic
  def add(value, rql() = query) do
    new_term(:'ADD', [value], query)
  end

  def sub(value, rql() = query) do
    new_term(:'SUB', [value], query)
  end

  def mul(value, rql() = query) do
    new_term(:'MUL', [value], query)
  end

  def div(value, rql() = query) do
    new_term(:'DIV', [value], query)
  end

  def mod(value, rql() = query) do
    new_term(:'MOD', [value], query)
  end

  def _and(value, rql() = query) do
    new_term(:'ALL', [value], query)
  end

  def _or(value, rql() = query) do
    new_term(:'ANY', [value], query)
  end

  def _not(rql() = query) do
    new_term(:'NOT', [], query)
  end

  def eq(value, rql() = query) do
    new_term(:'EQ', [value], query)
  end

  def ne(value, rql() = query) do
    new_term(:'NE', [value], query)
  end

  def gt(value, rql() = query) do
    new_term(:'GT', [value], query)
  end

  def ge(value, rql() = query) do
    new_term(:'GE', [value], query)
  end

  def lt(value, rql() = query) do
    new_term(:'LT', [value], query)
  end

  def le(value, rql() = query) do
    new_term(:'LE', [value], query)
  end

  # Document Manipulation
  def row do
    new_term(:'IMPLICIT_VAR', [])
  end

  def merge(object, rql() = query) do
    new_term(:'MERGE', [object], [], query)
  end

  def append(value, rql() = query) do
    new_term(:'APPEND', [value], query)
  end

  def prepend(value, rql() = query) do
    new_term(:'PREPEND', [value], query)
  end

  def has_fields(selectors, rql() = query) do
    new_term(:'HAS_FIELDS', [selectors], query)
  end

  # Control Structures
  def info(rql() = query) do
    new_term(:'INFO', [], query)
  end

  def expr(Range[] = range) do
    make_array(Enum.to_list(range))
  end

  def expr(rql() = query), do: query
  def expr([head|_] = value) when is_tuple(head) do
    expr(HashDict.new(value))
  end

  def expr(value) when is_record(value, HashDict) do
    make_obj(value)
  end

  def expr(values) when is_list(values) do
    make_array(values)
  end

  def expr(value), do: new_term(:EXPR, [value])

  @doc """
  Create a new connection to the database server

  ## Example

  Opens a connection using the default host and port but specifying
  the default database.

      iex> conn = r.connect(db: "heroes")
  """
  @spec connect(params | url) :: conn
  def connect(opts // []) do
    Rethinkdb.Connection.new(opts).connect!
  end

  # Build a rql terms in a ql2 terms
  @doc false
  def build(rql(terms: terms)) do
    Enum.reduce terms, nil, build_terms(&1, &2)
  end

  defp build_term_datum(value) do
    try do
      Term.new(type: :'DATUM', datum: Datum.from_value(value))
    rescue
      CaseClauseError ->
        IO.inspect(value)
    end
  end

  defp build_terms(term(type: :'EXPR', args: [value]), _left) do
    build_term_datum(value)
  end

  defp build_terms(term(type: type, args: args, optargs: optargs), left) do
    optargs = format_opts(optargs)
    args    = format_args(args)
    if left != nil, do: args = [left | args]
    Term.new(type: type, args: args, optargs: optargs)
  end

  defp format_args(args) do
    lc arg inlist args, do: format_arg(arg)
  end

  defp format_opts(args) when is_record(args, HashDict) do
    format_opts(args.to_list)
  end

  defp format_opts(optargs) do
    lc {key, value} inlist optargs do
      Term.AssocPair.new(key: "#{key}", val: format_arg(value))
    end
  end

  defp format_arg(arg) do
    case arg do
      rql()  = rql  -> build(rql)
      term() = term -> build_terms(term, nil)
      arg -> build_term_datum(arg)
    end
  end

  # Helper to terms create
  defp new_term(type, args // []) do
    new_term(type, args, [], rql())
  end

  defp new_term(type, args, nil) do
    new_term(type, args, [], rql())
  end

  defp new_term(type, args, rql() = query) do
    new_term(type, args, [], query)
  end

  defp new_term(type, args, opts) when is_list(opts) or is_record(opts, HashDict) do
    new_term(type, args, opts, rql())
  end

  defp new_term(type, args, optargs, rql(terms: terms)) do
    rql(terms: terms ++ [term(type: type, args: args, optargs: optargs)])
  end

  defp make_array(items) when is_list(items) do
    new_term(:'MAKE_ARRAY', items)
  end

  defp make_obj(values) do
    new_term(:'MAKE_OBJ', [], values)
  end

  defp var(n) do
    new_term(:'VAR', [n])
  end

  def access({Range, start_index, end_index} = ranger, rql() = query) do
    opts = case end_index do
      n when n < 0 -> [right_bound: :closed]
      _ -> []
    end
    new_term(:'SLICE', [start_index, end_index], opts, query)
  end

  def access(key, rql() = query) do
    new_term(:'GET_FIELD', [key], [], query)
  end

  # Function helpers
  def func(func) do
    {_, arity} = :erlang.fun_info(func, :arity)
    arg_count  = :lists.seq(1, arity)
    func_args  = lc n inlist arg_count, do: var(n)

    args = case apply(func, func_args) do
      [{_, _}|_] = obj -> [new_term(:'MAKE_OBJ', [], obj)]
      rql() = query -> [query]
    end

    new_term(:'FUNC', [expr(arg_count) | args])
  end
end
