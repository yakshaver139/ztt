import signal
import unittest
from unittest.mock import Mock, patch

from jobs.run_power_ingest import main


class SupervisorTest(unittest.TestCase):
    def test_kill_signals_restart_only_their_target(self) -> None:
        for kill_signal, killed_name in ((signal.SIGURG, "worker"),
                                         (signal.SIGUSR1, "collector")):
            with self.subTest(killed_name=killed_name):
                collector = Mock(pid=1, exitcode=None)
                worker = Mock(pid=2, exitcode=None)
                replacement = Mock(pid=3, exitcode=None)
                for child in (collector, worker, replacement):
                    child.is_alive.return_value = False
                victim, survivor = ((worker, collector) if killed_name == "worker"
                                    else (collector, worker))
                victim.kill.side_effect = lambda: setattr(victim, "exitcode", -9)
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
                        handlers[kill_signal](None, None)
                    else:
                        raise KeyboardInterrupt

                with patch.dict("os.environ", {"BMS_TOKEN": "test",
                                            "BMS_FILTER_POWER_SPIKES": "0"}, clear=True), \
                        patch("jobs.run_power_ingest.get_context", return_value=context), \
                        patch("jobs.run_power_ingest.signal.signal", side_effect=register), \
                        patch("jobs.run_power_ingest.time.sleep", side_effect=sleep):
                    main()

                victim.kill.assert_called_once_with()
                survivor.kill.assert_not_called()
                replacement.start.assert_called_once_with()
                self.assertEqual(context.Process.call_args_list[2].kwargs["name"], killed_name)
                expected_args = ((1, 30.0, 3600.0, False) if killed_name == "worker"
                                 else ("test", 1, None))
                self.assertEqual(context.Process.call_args_list[2].kwargs["args"], expected_args)

    def test_rejects_invalid_spike_filter_flag(self) -> None:
        with patch.dict("os.environ", {"BMS_TOKEN": "test",
                                    "BMS_FILTER_POWER_SPIKES": "maybe"}):
            with self.assertRaisesRegex(SystemExit, "BMS_FILTER_POWER_SPIKES must be 0 or 1"):
                main()


if __name__ == "__main__":
    unittest.main()
