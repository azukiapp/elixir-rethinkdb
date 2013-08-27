defmodule QL2 do
  use Protobuf, from: Path.expand("../proto/ql2.proto", __DIR__)

  extra_block "Datum" do
    Record.import __MODULE__, as: :record

    def value(record() = datum) do
      Rethinkdb.DatumHelpers.decode(datum)
    end

    def from_value(value) do
      Rethinkdb.DatumHelpers.encode(value)
    end
  end

  def global_database(database) do
    __MODULE__.Query.AssocPair.new(key: "db", val:
      __MODULE__.Term.new(type: :'DATUM', datum:
        __MODULE__.Datum.new(type: :'R_STR', r_str: database)
      )
    )
  end
end
