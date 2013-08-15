defmodule Rexthinkdb.Mixfile do
  use Mix.Project

  def project do
    [ app: :rexthinkdb,
      version: "0.0.1",
      elixir: "~> 0.10.1",
      compilers: [:protobuffs, :elixir, :app],
      deps: deps(Mix.env) ]
  end

  # Configuration for the OTP application
  def application do
    [
      mod: { Rethinkdb.App, [] },
      applications: [:socket],
      env: Keyword.merge([{:timeout, 30 * 1000}], env(Mix.env))
    ]
  end

  def env(:test) do
    [rethinkdb_uri: "rethinkdb://localhost:28015/test_rexthinkdb"]
  end

  def env(_), do: []

  # Returns the list of dependencies in the format:
  def deps(:prod) do
    [
      { :meck, github: "eproxus/meck", branch: "develop", override: true },
      { :mix_protobuffs, "~> 0.9.0", git: "git://github.com/nuxlli/mix_protobuffs.git", branch: "fixing_use_mix_code_erlang"},
      { :protobuffs, "~> 0.8.0", git: "git://github.com/basho/erlang_protobuffs.git" },
      { :socket, github: "meh/elixir-socket" }
    ]
  end

  def deps(:test) do
    deps(:prod) ++ [
      {:exmeck, github: "azukiapp/exmeck"},
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
