defmodule ElasticClient.HTTP do
  @moduledoc """
  Provides a simple api to make calls to an ElasticSearch server over HTTP.
  
  """
  alias HTTPoison, as: Http
  require Logger

  @recv_timeout 60000

  @doc """
  Checks if the provided index name exists on the server and returns a boolean result.
  """
  @spec index_exists?(String.t()) :: boolean()
  def index_exists?(index_name) when is_binary(index_name)do
    case http_head(index_url(index_name)) do
      {:ok, 200, _} -> true
      _ -> false
    end
  end

  @doc """
  Checks if the provided mapping name exists on the index name and returns a boolean result.
  """
  @spec mapping_exists?(String.t(), String.t()) :: boolean()
  def mapping_exists?(index_name, mapping_name) do
    case http_head("#{index_url(index_name)}/_mapping/#{mapping_name}") do
      {:ok, 200, _} -> true
      _ -> false
    end
  end

  @spec add_index(String.t()) :: {atom}
  def add_index(index_name), do: add_index(index_name, "")

  @spec add_index(String.t(), String.t()) :: {atom(), any()}
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
  
  @type bulk_option :: {:id, String.t()} | {:index_name, String.t()} | {:doc_type, String.t()}
  @spec bulk_action(
    atom(),
    String.t(),
    [bulk_option]
  ) :: [any()]
  @doc """
  builds an es bulk action pair to index a document
  params:
  doc - the jsoninfied document to index
  options:
  index_name - the index name to send address the bulk action to
  doc_type - the document type to adress the document to
  id - if this is a reindex of an existing document, put the es _id in this key
  """
  def bulk_action(:index, doc, opts) do
    base =
      case opts[:id] do
        nil ->
          %{index: %{_index: index(opts[:index_name]),_type: opts[:doc_type]}}
        new_id ->
          %{index: %{_id: new_id, _index: index(opts[:index_name]),_type: opts[:doc_type]}}
      end
    [base, doc]
  end

  @doc """
  builds an es bulk action pair to update a document
  params:
  doc = jsonified document to save as the udpate
  options:
  index_name - the index name to send address the bulk action to
  doc_type - the document type to adress the document to
  id - the es _id for the document being updated
  """
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

  @doc """
  builds an es bulk action to delete a document
  params:
  id - the es _id for the document being updated
  options:
  index_name - the index name to send address the bulk action to
  doc_type - the document type to adress the document to
  """
  def bulk_action(:delete, id, opts) do
    base =
      %{
        delete: %{
          _index: index(opts[:index_name]),
          _type: opts[:doc_type],
          _id: id
        }
      }
    [base]
  end

  def alias_action(type, opts) when type in [:add_alias, :remove_alias] do
    base = 
      %{
        index: opts[:index_name],
        alias: opts[:alias_name],
      }
    case type do
      :add_alias -> Map.put(%{}, :add, base)
      :remove_alias -> Map.put(%{}, :remove, base)
    end
  end

  @doc """
  adds a mapping definition to an index
  params:
  label - the label to use for the mapping, will be the type
  mapping - the mapping definition itself
  """
  @spec add_mapping(String.t(), String.t(), map()) :: {:ok, any()} | {:error, any()}
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

  @doc """
  index a new document under the given index name as the given type
  params:
  index_name - the name of the index to index the document under
  document - the jsonified document to index
  type - the type to use when indexing the document, there
  should already be a mapping in place on the given index name for this type
  """
  @spec index_document(String.t(), String.t(), String.t()) :: {:ok, any()} | {:error, any()}
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

  @doc """
  perform a GET search on the given index name and type using the urlencoded query.
  returns results of a call to search_results which is a map
  params:
  index_name - the index name to run the search query on
  type - the type to run the search on
  query - a map with key/value search params
  """
  @spec search(atom(), String.t(), String.t(), map()) :: map()
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

  @doc """
  perform a POST search on the given index name and passing the query map as a
  json encoded POST payload.
  params:
  index_name - the index name to run the search query on
  type - the type to run the search on
  query - a map with key/value search params
  """
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

  # calls to _bulk api
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

    result = http_post(url, actions, headers)

    case result do
      {:ok, 200, body} -> {:ok, Jason.decode!(body)}
      {:ok, sc} -> {:error, sc}
      {:error, reason} -> {:error, reason}
    end
  end

  # calls to _alias api
  def do_alias_actions(actions) when is_list(actions) do
    Jason.encode!(%{actions: actions})
    |> do_alias_actions()
  end

  def do_alias_actions(actions) when is_binary(actions) do
    headers = [{"content-type", "application/x-ndjson"}]
    url ="#{host_url()}/_aliases"
    result = http_post(url, actions, headers)
    case result do
      {:ok, 200, body} -> {:ok, Jason.decode!(body)}
      {:ok, sc} -> {:error, sc}
      {:error, reason} -> {:error, reason}
    end
  end

  def delete_by_query(index_name, type, q, refresh \\ true) do
    tail =
      case refresh do
        true -> "refresh"
        _ -> ""
      end
    url = "#{url_for(index_name, type)}/_delete_by_query?#{tail}"
    case http_post(url, q |> Jason.encode!()) do
      {:ok, 200, body} -> {:ok, Jason.decode!(body)}
      {:ok, sc} -> {:error, sc}
      {:error, reason} -> {:error, reason}
    end
  end

  def alias(alias_name, index_name, re_alias) do
    base = alias_action(:add_alias, index_name: index_name, alias_name: alias_name)
    alias_actions =
      case re_alias do
        true ->
          case fetch_alias_indices(alias_name) do
            [] ->
              [base]
            existing ->
              expanded =
                Enum.map(existing, fn existing_aliased ->
                  alias_action(
                    :remove_alias,
                    index_name: existing_aliased,
                    alias_name: alias_name
                  )
                end)
                [base | expanded]
                |> Enum.reverse()
          end
        _ ->
          [base]
      end
    do_alias_actions(alias_actions)
  end

  def fetch_alias_indices(alias_name) do
    url = "#{host_url()}/*/_alias/#{alias_name}"
    case http_get(url) do
      {:ok, 200, body} ->
        Jason.decode!(body)
        |> Enum.map(&(elem(&1, 0)))
      _ ->
        []
        
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
  defp http_post(url, req), do: http_post(url, req, headers())
  defp http_post(url, req, headers) do
    Http.post(url, req, headers, [recv_timeout: @recv_timeout]) 
    |> parse_http_response()
  end
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

  defp headers(), do: [{"content-type", "application/json"}]

  defp host(), do: config()[:host]
  defp port(), do: config()[:port]
  defp index(index_name), do: "#{config()[:index_prefix]}#{index_name}"

  defp config() do
    Application.get_env(:elastic_client, :server)
  end
end
