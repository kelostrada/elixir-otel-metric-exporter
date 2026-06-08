defmodule OtelMetricExporter.LogHandlerGuardianTest do
  use ExUnit.Case, async: false

  alias OtelMetricExporter.LogHandlerGuardian

  defmodule DummyHandler do
    @behaviour :logger_handler

    @impl true
    def adding_handler(config), do: {:ok, config}

    @impl true
    def removing_handler(_config), do: :ok

    @impl true
    def log(_event, _config), do: :ok
  end

  setup do
    guardian =
      case Process.whereis(LogHandlerGuardian) do
        nil ->
          start_supervised!({LogHandlerGuardian, interval_ms: 60_000})
          Process.whereis(LogHandlerGuardian)

        pid ->
          pid
      end

    id = :guardian_test_handler

    on_exit(fn ->
      LogHandlerGuardian.unwatch(id)
      :logger.remove_handler(id)
    end)

    %{guardian: guardian, id: id}
  end

  # Force the guardian to process its mailbox (casts + the :check we send),
  # so the tests are deterministic without sleeping on the timer.
  defp reconcile(guardian) do
    :sys.get_state(guardian)
    send(guardian, :check)
    :sys.get_state(guardian)
  end

  test "re-adds a watched handler after it is detached", %{guardian: guardian, id: id} do
    :ok = :logger.add_handler(id, DummyHandler, %{})
    LogHandlerGuardian.watch(id, DummyHandler, %{})

    :ok = :logger.remove_handler(id)
    refute id in :logger.get_handler_ids()

    reconcile(guardian)
    assert id in :logger.get_handler_ids()
  end

  test "leaves an unwatched handler detached", %{guardian: guardian, id: id} do
    :ok = :logger.add_handler(id, DummyHandler, %{})
    :ok = :logger.remove_handler(id)

    reconcile(guardian)
    refute id in :logger.get_handler_ids()
  end

  test "unwatch stops the guardian from re-adding", %{guardian: guardian, id: id} do
    :ok = :logger.add_handler(id, DummyHandler, %{})
    LogHandlerGuardian.watch(id, DummyHandler, %{})
    LogHandlerGuardian.unwatch(id)

    :ok = :logger.remove_handler(id)

    reconcile(guardian)
    refute id in :logger.get_handler_ids()
  end

  test "keeps an already-attached watched handler in place", %{guardian: guardian, id: id} do
    :ok = :logger.add_handler(id, DummyHandler, %{})
    LogHandlerGuardian.watch(id, DummyHandler, %{})

    reconcile(guardian)
    assert id in :logger.get_handler_ids()
  end
end
