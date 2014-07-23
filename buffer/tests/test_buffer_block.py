from ..buffer_block import Buffer
from unittest.mock import MagicMock
from nio.util.support.block_test_case import NIOBlockTestCase
from nio.common.signal.base import Signal
from nio.modules.threading import Event


class EventBuffer(Buffer):
    def __init__(self, event):
        super().__init__()
        self._event = event

    def emit(self, reset=False):
        super().emit(reset)
        self._event.set()


class TestBuffer(NIOBlockTestCase):
    
    def test_buffer(self):
        event = Event()
        block = EventBuffer(event)
        block._backup = MagicMock()
        self.configure_block(block, {
            "interval": {
                "milliseconds": 200
            }
        })
        block.start()
        block.process_signals([Signal(), Signal(), Signal(), Signal()])
        self.assert_num_signals_notified(0, block)
        event.wait(.3)
        self.assert_num_signals_notified(4, block)
        block.stop()

    def test_timeout(self):
        event = Event()
        block = EventBuffer(event)
        block._backup = MagicMock()
        self.configure_block(block, {
            "interval": {
                "milliseconds": 200
            },
            "timeout": True
        })
        block.start()
        event.wait(.3)
        self.assert_num_signals_notified(1, block)
        block.stop()
