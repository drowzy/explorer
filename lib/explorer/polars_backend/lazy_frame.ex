defmodule Explorer.PolarsBackend.LazyFrame do
  alias Explorer.DataFrame
  alias Explorer.PolarsBackend.Shared
  defstruct [:resource, :reference]

  ## alias Explorer.DataFrame
  ## alias Explorer.PolarsBackend.LazyFrame
  ## df = Explorer.DataFrame.from_map(%{a: ["a", "b", "c"], b: [1, 2, 3], c: [4, 5, 6]})
  ## plan = df |> LazyFrame.from_dataframe() |> LazyFrame.select(["a"]) |> LazyFrame.describe()
  ## new_lf = LazyFrame.select(lf)
  def from_dataframe(%DataFrame{} = df) do
    Shared.apply_native(df, :lf_lazy, [])
  end

  def select(%__MODULE__{} = lf, sel) do
    Shared.apply_native(lf, :lf_select, [sel])
  end
  def collect(%__MODULE__{} = lf) do
    Shared.apply_native(lf, :lf_collect, [])
  end

  def describe(%__MODULE__{} = lf) do
    Shared.apply_native(lf, :lf_describe_plan, [])
  end
end
