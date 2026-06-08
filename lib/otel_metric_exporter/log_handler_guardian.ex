defmodule OtelMetricExporter.LogHandlerGuardian do
  @moduledoc """
  Re-attaches `OtelMetricExporter.LogHandler` instances after the OTP logger
  detaches them.

  `LogHandler` runs its OLP / accumulator under a `:temporary` supervisor with
  `auto_shutdown: :any_significant` (see `OtelMetricExporter.LogHandlerSupervisor`).
  When the OLP process terminates for any reason — an overload kill, or a crash
  while exporting during a collector / gateway outage — that supervisor shuts
  down and `:logger_handler_watcher` permanently detaches the handler: no
  restart, no re-attach, until the next full application boot.

  Restarting the OLP in place is not sufficient: `LogHandler` caches the OLP
  handle in its handler config (`adding_handler/1`) and `log/2` asserts the
  cached pid is alive, so a replaced OLP would only make `log/2` crash and get
  the handler detached again. The only robust recovery is to **re-add** the
  handler, which re-runs `adding_handler/1` and caches a fresh OLP.

  This GenServer remembers the user-facing config of every `LogHandler` that was
  added (registered from `LogHandler.adding_handler/1`) and, on a fixed
  interval, re-adds any whose id is no longer present in
  `:logger.get_handler_ids/0`.

  ## Configuration

      config :otel_metric_exporter,
        # master switch; set to false to restore the stock detach-and-stay-gone
        # behaviour (default: true)
        reattach_detached_log_handlers: true,
        # how often to reconcile attached handlers, in milliseconds
        log_handler_guardian_interval_ms: 5_000

  To deliberately remove a guarded handler, call `unwatch/1` before
  `:logger.remove_handler/1`, otherwise the guardian will re-add it on the next
  tick.
  """

  use GenServer
  require Logger

  @default_interval_ms 5_000

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  Register a handler so the guardian re-adds it if the logger detaches it.

  `config` must be the map accepted by `:logger.add_handler/3` (i.e. without the
  `:id` / `:module` keys). No-op when the guardian process is not running (e.g.
  in tests that do not start the `:otel_metric_exporter` application).
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
