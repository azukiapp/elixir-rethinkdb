ExUnit.start

defmodule Rethinkdb.Case do
  use ExUnit.CaseTemplate

  using _ do
    quote do
      import unquote(__MODULE__)
      require Exmeck
    end
  end
end
