defmodule OtelMetricExporter.LogHandlerGuardian do
  @moduledoc """
  Re-attaches `OtelMetricExporter.LogHandler` instances that the OTP logger
  detaches when their OLP supervisor auto-shuts-down (e.g. on a crash during a
  gateway outage). Re-adding re-runs `adding_handler/1`, which is the only way
  to recover: the handler caches the OLP handle, so restarting the OLP in place
  leaves a stale pid that `log/2` rejects.

  Handlers register themselves from `LogHandler.adding_handler/1`; this server
  re-adds any whose id is missing from `:logger.get_handler_ids/0` on each tick.

  ## Configuration

      config :otel_metric_exporter,
        reattach_detached_log_handlers: true,
        log_handler_guardian_interval_ms: 5_000

  Call `unwatch/1` before `:logger.remove_handler/1` to remove a guarded handler
  for good.
  """

  use GenServer
  require Logger

  @default_interval_ms 5_000

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  Register a handler so the guardian re-adds it if the logger detaches it.
  `config` is the map passed to `:logger.add_handler/3` (no `:id` / `:module`).
  No-op when the guardian is not running.
  """
  @spec watch(:logger.handler_id(), module(), map()) :: :ok
  def watch(id, module, config) do
    case Process.whereis(__MODULE__) do
      nil -> :ok
      _ -> GenServer.cast(__MODULE__, {:watch, id, module, config})
    end
  end

  @doc """
  Stop guarding a handler so a deliberate `:logger.remove_handler/1` sticks.
  """
  @spec unwatch(:logger.handler_id()) :: :ok
  def unwatch(id) do
    case Process.whereis(__MODULE__) do
      nil -> :ok
      _ -> GenServer.cast(__MODULE__, {:unwatch, id})
    end
  end

  @impl true
  def init(opts) do
    interval = Keyword.get(opts, :interval_ms, configured_interval())
    {:ok, schedule(%{interval: interval, handlers: %{}})}
  end

  @impl true
  def handle_cast({:watch, id, module, config}, state) do
    {:noreply, put_in(state.handlers[id], {module, config})}
  end

  def handle_cast({:unwatch, id}, state) do
    {:noreply, %{state | handlers: Map.delete(state.handlers, id)}}
  end

  @impl true
  def handle_info(:check, state) do
    present = MapSet.new(:logger.get_handler_ids())

    Enum.each(state.handlers, fn {id, {module, config}} ->
      unless MapSet.member?(present, id), do: reattach(id, module, config)
    end)

    {:noreply, schedule(state)}
  end

  def handle_info(_msg, state), do: {:noreply, state}

  defp reattach(id, module, config) do
    case :logger.add_handler(id, module, config) do
      :ok ->
        Logger.warning("[OtelMetricExporter] re-attached detached log handler #{inspect(id)}")

      {:error, {:already_exist, _}} ->
        :ok

      {:error, reason} ->
        Logger.error(
          "[OtelMetricExporter] failed to re-attach log handler #{inspect(id)}: #{inspect(reason)}"
        )
    end
  end

  defp schedule(%{interval: interval} = state) do
    Process.send_after(self(), :check, interval)
    state
  end

  defp configured_interval do
    Application.get_env(
      :otel_metric_exporter,
      :log_handler_guardian_interval_ms,
      @default_interval_ms
    )
  end
end
