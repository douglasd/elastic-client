defmodule ElasticClient.QueryBuilder do
  
    def match(field, value) do
      %{match: Map.put(%{}, field, value)}
    end

    def term(field, value) do
      %{term: Map.put(%{}, field, value)}
    end
  
    def range(opts \\ []) do
      range_opts = ~w(gt gte lt lte)a
      operators =
        Enum.into(opts, %{})
        |> Map.take(range_opts)
      Map.put(%{}, :range, Map.put(%{}, opts[:field_name], operators))
    end
  
    def query(clauses \\ []) do
      clause_keys = ~w(bool)a
      query_clauses = 
        Enum.reduce(clauses, %{}, fn {k, v}, q ->
          case k in clause_keys do
            true -> Map.put(q, k, v.bool)
            _ -> q
          end
        end)
      %{query: query_clauses}
    end
  
    def bool(opts \\ []) do
      # boolean clause that will accept must, must_not and should clauses
      opt_keys = ~w(must must_not should)a
      clauses =
        Enum.into(opts, %{})
        |> Map.take(opt_keys)
      %{bool: clauses}
    end
    
    def nested(path, query) do
      %{nested: Map.put(query, :path, path)}
    end
  
  
    ## aggregations pipeline
    def terms_aggregation(aggs, label, terms, child_aggs \\ nil) do
      single_field_aggregation(aggs, :terms, label, terms, child_aggs)
    end
    def cardinality_aggregation(aggs, label, terms, child_aggs \\ nil) do
      single_field_aggregation(aggs, :cardinality, label, terms, child_aggs)
    end
  
    def single_field_aggregation(aggs, type, label, terms, child_aggs \\ nil) do
      base = Map.put(%{}, type, terms)
      agg =
        case child_aggs do
          nil -> base
          _ -> Map.put(base, :aggs, child_aggs)
        end
      Map.put(aggs, label, agg)
    end
  
    def bucket_selector_aggregation(aggs, label, {k, v}, operator, value) do
      s = "params.#{k} #{operator} #{value}"
      Map.put(aggs, label, %{bucket_selector: %{buckets_path: Map.put(%{}, k, v), script: s}})
    end

end