defmodule Rethinkdb.Mixfile do
  use Mix.Project

  def project do
    [ app: :'rethinkdb',
      version: "0.2.2",
      elixir: "~> 0.10.2",
      deps: deps(Mix.env) ]
  end

  # Configuration for the OTP application
  def application do
    [
      mod: { Rethinkdb.App, [] },
      applications: [],
      env: Keyword.merge([{:timeout, 30}], env(Mix.env))
    ]
  end

  def env(:test) do
    [rethinkdb_uri: "rethinkdb://localhost:28015/elixir_drive_test"]
  end

  def env(_), do: []

  # Returns the list of dependencies in the format:
  def deps(:prod) do
    [
      { :protobuf, github: "azukiapp/elixir-protobuf", tag: "v0.0.2" },
    ]
  end

  def deps(:test) do
    deps(:prod) ++ [
      {:meck, github: "eproxus/meck", tag: "0.8.1", override: true},
      {:mock, github: "jjh42/mock"}
    ]
  end

  def deps(:docs) do
    deps(:prod) ++
      [ { :ex_doc, github: "elixir-lang/ex_doc" } ]
  end

  def deps(_) do
    deps(:prod)
  end
end
