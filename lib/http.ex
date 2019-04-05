defmodule ElasticClient.HTTP do
  alias HTTPoison, as: Http
  require Logger

  def index_exists(index_name) do
    case http_head(index_url(index_name)) do
      {:ok, 200, _} -> true
      _ -> false
    end
  end

  def mapping_exists(index_name, mapping_name) do
    case http_head("#{index_url(index_name)}/_mapping/#{mapping_name}") do
      {:ok, 200, _} -> true
      _ -> false
    end
  end

  def add_index(index_name), do: add_index(index_name, "")

  def add_index(index_name, mapping) do
    case http_put(index_url(index_name), mapping) do
      {:ok, 200, body} ->
        {:ok, body |> Jason.decode!()}

      {:ok, sc} ->
        {:error, sc}

      {:error, reason} ->
        {:error, reason}
    end
  end

  def bulk_action(:index, doc, opts) do
    base = %{
      index: %{
        _index: index(opts[:index_name]),
        _type: opts[:doc_type]
      }
    }

    case opts[:id] do
      nil ->
        [base, doc]

      new_id ->
        [Map.put(base, :_id, new_id), doc]
    end
  end

  def bulk_action(:update, doc, opts) do
    base = %{
      update: %{
        _index: index(opts[:index_name]),
        _type: opts[:doc_type],
        _id: opts[:id]
      }
    }

    [base, %{doc: doc}]
  end

  def add_mapping(index_name, label, mapping) do
    url = "#{index_url(index_name)}/_mapping/#{label}"

    case http_put(url, mapping) do
      {:ok, 200, body} ->
        {:ok, body |> Jason.decode!()}

      {:ok, sc} ->
        {:error, sc}

      {:error, reason} ->
        {:error, reason}
    end
  end

  def index_document(index_name, document, type) do
    case http_post(url_for(index_name, type), document) do
      {:ok, 200, body} ->
        {:ok, body |> Jason.decode!()}

      {:ok, sc} ->
        {:error, sc}

      {:error, reason} ->
        {:error, reason}
    end
  end

  def search(:get, index_name, type, query) when is_binary(type) and is_map(query) do
    url = url_for(index_name, type) <> "/_search?" <> URI.encode_query(query)

    case http_get(url) do
      {:ok, 200, body} ->
        search_results(body |> Jason.decode!())

      {:ok, _sc} ->
        search_results()

      {:error, _reason} ->
        search_results()
    end
  end

  def search(:post, index_name, type, query) when is_binary(type) do
    url = url_for(index_name, type) <> "/_search"

    case http_post(url, query |> Jason.encode!()) do
      {:ok, 200, body} ->
        search_results(body |> Jason.decode!())

      {:ok, _sc} ->
        search_results()

      {:error, _reason} ->
        search_results()
    end
  end

  def delete(index_name, type, id) do
    case http_delete("#{url_for(index_name, type)}/#{id}") do
      {:ok, 200, body} ->
        {:ok, body |> Jason.decode!()}

      {:ok, sc} ->
        {:error, sc}

      {:error, reason} ->
        {:error, reason}
    end
  end

  def do_bulk_actions(actions, refresh \\ true)

  def do_bulk_actions(actions, refresh) when is_list(actions) do
    Enum.reduce(actions, "", fn i, acc ->
      "#{acc}#{Jason.encode!(i)}\n"
    end)
    |> do_bulk_actions(refresh)
  end

  def do_bulk_actions(actions, refresh) when is_binary(actions) do
    headers = [{"content-type", "application/x-ndjson"}]

    url =
      case refresh do
        true -> "#{host_url()}/_bulk?refresh"
        _ -> "#{host_url()}/_bulk"
      end

    result = Http.post(url, actions, headers)

    case parse_http_response(result) do
      {:ok, 200, body} -> {:ok, Jason.decode!(body)}
      {:ok, sc} -> {:error, sc}
      {:error, reason} -> {:error, reason}
    end
  end

  def hits(%{"hits" => hits}), do: hits

  def source(%{"_source" => source}), do: source

  def index_url(index_name) do
    "http://#{host()}:#{port()}/#{index(index_name)}"
  end

  def host_url(), do: "http://#{host()}:#{port()}"

  def url_for(index_name, type), do: "#{index_url(index_name)}/#{type}"
  def url_for(index_name, type, id), do: "#{url_for(index_name, type)}/#{id}"

  defp http_get(url), do: Http.get(url, headers()) |> parse_http_response()
  defp http_post(url, req), do: Http.post(url, req, headers()) |> parse_http_response()
  defp http_put(url, req), do: Http.put(url, req, headers()) |> parse_http_response()
  defp http_head(url), do: Http.head(url, headers()) |> parse_http_response()
  defp http_delete(url), do: Http.delete(url, headers()) |> parse_http_response()

  defp parse_http_response(resp) do
    case resp do
      {:ok, %Http.Response{status_code: 200, body: body}} ->
        {:ok, 200, body}

      {:ok, %Http.Response{status_code: 404}} ->
        {:ok, 404}

      {:ok, %Http.Response{status_code: sc}} ->
        {:ok, sc}

      {:error, %Http.Error{reason: reason}} ->
        {:error, reason}
    end
  end

  defp search_results, do: %{meta: %{total: 0}, hits: []}

  defp search_results(%{"hits" => %{"total" => total, "hits" => hits}, "aggregations" => aggs}) do
    %{meta: %{total: total, aggs: aggs}, hits: hits}
  end

  defp search_results(%{"hits" => %{"total" => total, "hits" => hits}}) do
    %{meta: %{total: total}, hits: hits}
  end

  # defp update_query(script, query) do
  #   %{
  #     conflicts: "proceed",
  #     script: %{
  #       source: script,
  #       lang: "painless"
  #     },
  #     query: query
  #   }
  #   |> Jason.encode!()
  # end

  defp headers(), do: [{"content-type", "application/json"}]

  defp host(), do: config()[:host]
  defp port(), do: config()[:port]
  defp index(index_name), do: "#{config()[:index_prefix]}#{index_name}"

  defp config() do
    Application.get_env(:elasticsearch_client)
  end
end
