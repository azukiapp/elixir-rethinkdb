defmodule QL2 do
  alias Rethinkdb.Utils

  use Protobuf, from: Path.expand("../proto/ql2.proto", __DIR__)

  use_in :Datum, QL2.DatumHelpers
  use_in :Response, QL2.ResponseHelpers

  def version do
    QL2.VersionDummy.Version.value(:V0_2)
  end

  def global_database(database) do
    __MODULE__.Query.AssocPair.new(key: "db", val:
      __MODULE__.Term.new(type: :'DB', args:
        [__MODULE__.Term.new(type: :'DATUM', datum:
          __MODULE__.Datum.new(type: :'R_STR', r_str: database)
        )]
      )
    )
  end
end
