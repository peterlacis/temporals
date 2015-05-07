from ..debounce.debounce_block import Debouncer
from unittest.mock import MagicMock
from nio.util.support.block_test_case import NIOBlockTestCase
from nio.common.signal.base import Signal
from nio.modules.threading import sleep


class TestDebounce(NIOBlockTestCase):

    def get_test_modules(self):
        return self.ServiceDefaultModules + ['persistence']

    def test_debounce(self):
        block = Debouncer()
        block._backup = MagicMock()
        self.configure_block(block, {
            "interval": {
                "milliseconds": 200
            }
        })
        block.start()
        block.process_signals([Signal(), Signal()])
        block.process_signals([Signal()])
        self.assert_num_signals_notified(1, block)
        sleep(.3)
        block.process_signals([Signal()])
        self.assert_num_signals_notified(2, block)
        block.stop()

    def test_debounce_group(self):
        block = Debouncer()
        block._backup = MagicMock()
        self.configure_block(block, {
            "interval": {
                "milliseconds": 200
            },
            "group_by": "{{$foo}}"
        })
        block.start()
        block.process_signals([
            Signal({'foo': 'bar'}),
        ])
        self.assert_num_signals_notified(1, block)
        block.process_signals([
            Signal({'foo': 'bar'}),
            Signal({'foo': 'qux'})
        ])
        self.assert_num_signals_notified(2, block)
        sleep(.3)
        block.process_signals([Signal({'foo': 'bar'})])
        self.assert_num_signals_notified(3, block)
        block.stop()
