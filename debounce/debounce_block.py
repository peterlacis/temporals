from datetime import datetime
from nio.common.block.base import Block
from nio.common.discovery import Discoverable, DiscoverableType
from nio.metadata.properties.timedelta import TimeDeltaProperty
from blocks.base_blocks.group_by.group_by_block import GroupBy


@Discoverable(DiscoverableType.block)
class Debouncer(Block, GroupBy):

    interval = TimeDeltaProperty()

    def __init__(self):
        Block.__init__(self)
        GroupBy.__init__(self)
        self._last_emission = {}

    def configure(self, context):
        super().configure(context)
        self._last_emission = self.persistence.load('last_emission') or {}

    def stop(self):
        self._backup()
    
    def process_signals(self, signals):
        self.for_each_group(signals, self.process_group)
        self.persist_if_dirty()

    def process_group(self, signals, key):
        now = datetime.utcnow()
        last_emission = self._last_emission.get(key)
        if last_emission is None or \
           now - last_emission > self.interval:
            self._logger.debug("Emitting a signal at %s" % now)
            self._last_emission[key] = now
            self._dirty = True
            self.notify_signals(signals[:1])

    def _store(self):
        self.persistence.store('last_emission', self._last_emission)

    def _backup(self):
        self.persistence.save()
        

        
