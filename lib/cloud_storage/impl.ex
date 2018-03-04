defmodule GCloudex.CloudStorage.Impl do
  @moduledoc """
  Wrapper for Google Cloud Storage API.
  """
  defmacro __using__(:cloud_storage) do
    quote location: :keep do
    end
  end
end
