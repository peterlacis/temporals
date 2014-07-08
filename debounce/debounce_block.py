from nio.common.block.base import Block
from nio.common.discovery import Discoverable, DiscoverableType
from nio.metadata.properties.timedelta import TimeDeltaProperty


@Discoverable(DiscoverableType.block)
class Debouncer(Block, GroupBlock):

    interval = TimeDeltaProperty()

    def __init__(self):
        self._last_emission = None

    def configure(self, context):
        self._last_emission = self.persistence.load('last_emission')
    
    def process_signals(self, signals):
        now = datetime.utcnow()
        if self._last_emission is None or \
           now - self._last_emission > self.interval:
            self._logger.debug("Emitting a signal at %s" % now)
            self.notify_signals(signals[:1])
            self.persistence.save('last_emission', now)
            self.persistence.store()
        
