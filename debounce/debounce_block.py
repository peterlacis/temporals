from datetime import datetime
from nio.common.block.base import Block
from nio.common.discovery import Discoverable, DiscoverableType
from nio.metadata.properties.timedelta import TimeDeltaProperty
from .block_supplements.group_by.group_by_block import GroupBy


@Discoverable(DiscoverableType.block)
class Debouncer(Block, GroupBy):

    """ A block to ensure that only one signal from each of the configured
    groups will be notified over the configured interval

    Properties:
        group_by (expression): The value by which signals are grouped.
        interval (timedelta): At most one signal from each group will 
            be emitted per this interval.

    """

    interval = TimeDeltaProperty()

    def __init__(self):
        Block.__init__(self)
        GroupBy.__init__(self)
        self._last_emission = {}
        self._dirty = False

    def configure(self, context):
        super().configure(context)
        self._last_emission = self.persistence.load('last_emission') or {}
        self._groups = self.persistence.load('groups') or []
    
    def process_signals(self, signals):
        self.for_each_group(self.process_group, signals)
        self._persist_if_dirty()

    def process_group(self, signals, key):
        now = datetime.utcnow()
        last_emission = self._last_emission.get(key)
        if last_emission is None or \
           now - last_emission > self.interval:
            self._logger.debug("Emitting a signal at %s" % now)
            self._last_emission[key] = now
            self._dirty = True
            self.notify_signals(signals[:1])

    def _persist_if_dirty(self):
        if self._dirty:
            self._store()
            self._backup()
            self._dirty = False

    def _store(self):
        self.persistence.store('last_emission', self._last_emission)
        self.persistence.store('groups', self._groups)

    def _backup(self):
        self.persistence.save()
        

        
