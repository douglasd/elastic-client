defmodule ElasticClientTest do
  use ExUnit.Case
  doctest ElasticClient

  test "greets the world" do
    assert ElasticClient.hello() == :world
  end
end
