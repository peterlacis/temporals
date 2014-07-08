from datetime import datetime
from nio.common.block.base import Block
from nio.common.discovery import Discoverable, DiscoverableType
from nio.metadata.properties.timedelta import TimeDeltaProperty


@Discoverable(DiscoverableType.block)
class Debouncer(Block):

    interval = TimeDeltaProperty()

    def __init__(self):
        super().__init__()
        self._last_emission = None

    def configure(self, context):
        super().configure(context)
        self._last_emission = self.persistence.load('last_emission')
    
    def process_signals(self, signals):
        now = datetime.utcnow()
        if self._last_emission is None or \
           now - self._last_emission > self.interval:
            self._logger.debug("Emitting a signal at %s" % now)
            self._last_emission = now
            self.persistence.store('last_emission', now)
            self.persistence.save()
            self.notify_signals(signals[:1])

        
