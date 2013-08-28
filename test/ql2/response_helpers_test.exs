defmodule QL2.ResponseHelpers.Test do
  use Rethinkdb.Case

  alias QL2.Response
  alias QL2.Datum

  test "return a datum value to SUCCESS_ATOM" do
    response = Response.new(type: :'SUCCESS_ATOM', response: [Datum.from_value(1)])
    assert {:ok, 1} = response.value
  end

  test "return a datum value to SUCCESS_PARTIAL" do
    response = Response.new(type: :'SUCCESS_PARTIAL', response: [Datum.from_value(1)])
    assert {:ok, 1} = response.value
  end

  test "return a array of object to SUCCESS_SEQUENCE" do
    data = [
      Datum.from_value(10),
      Datum.from_value("Foo bar")
    ]
    response = Response.new(type: :'SUCCESS_SEQUENCE', response: data)
    assert {:ok, [10, "Foo bar"]} == response.value
  end

  test "return a error to CLIENT_ERROR, COMPILE_ERROR, RUNTIME_ERROR" do
    {msg, backtrace} = {"msg of error", :backtrace}
    response = Response.new(
      response: [Datum.from_value(msg)],
      backtrace: backtrace
    )

    assert {:error, :'CLIENT_ERROR' , msg, backtrace} == response.type(:'CLIENT_ERROR').value
    assert {:error, :'COMPILE_ERROR', msg, backtrace} == response.type(:'COMPILE_ERROR').value
    assert {:error, :'RUNTIME_ERROR', msg, backtrace} == response.type(:'RUNTIME_ERROR').value
  end
end
