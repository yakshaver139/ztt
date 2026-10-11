import signal
import unittest
from unittest.mock import Mock, patch

from jobs.run_power_ingest import main


class SupervisorTest(unittest.TestCase):
    def test_worker_kill_signal_restarts_only_worker(self) -> None:
        collector = Mock(pid=1, exitcode=None)
        worker = Mock(pid=2, exitcode=None)
        replacement = Mock(pid=3, exitcode=None)
        collector.is_alive.return_value = False
        worker.is_alive.return_value = False
        replacement.is_alive.return_value = False
        worker.kill.side_effect = lambda: setattr(worker, "exitcode", -9)
        context = Mock()
        context.Process.side_effect = [collector, worker, replacement]
        handlers = {}
        sleeps = 0

        def register(sig, handler):
            handlers[sig] = handler
            return signal.SIG_DFL

        def sleep(_seconds):
            nonlocal sleeps
            sleeps += 1
            if sleeps == 1:
                handlers[signal.SIGURG](None, None)
            else:
                raise KeyboardInterrupt

        with patch.dict("os.environ", {"BMS_TOKEN": "test",
                                    "BMS_FILTER_POWER_SPIKES": "0"}), \
                patch("jobs.run_power_ingest.get_context", return_value=context), \
                patch("jobs.run_power_ingest.signal.signal", side_effect=register), \
                patch("jobs.run_power_ingest.time.sleep", side_effect=sleep):
            main()

        worker.kill.assert_called_once_with()
        collector.kill.assert_not_called()
        replacement.start.assert_called_once_with()
        self.assertEqual(context.Process.call_args_list[1].kwargs["args"],
                         (1, 30.0, 3600.0, False))

    def test_rejects_invalid_spike_filter_flag(self) -> None:
        with patch.dict("os.environ", {"BMS_TOKEN": "test",
                                    "BMS_FILTER_POWER_SPIKES": "maybe"}):
            with self.assertRaisesRegex(SystemExit, "BMS_FILTER_POWER_SPIKES must be 0 or 1"):
                main()


if __name__ == "__main__":
    unittest.main()
