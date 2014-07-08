from datetime import datetime
from nio.common.block.base import Block
from nio.common.discovery import Discoverable, DiscoverableType
from nio.metadata.properties.timedelta import TimeDeltaProperty


@Discoverable(DiscoverableType.block)
class Buffer(Block):

    interval = TimeDeltaProperty()

    def __init__(self):
        super().__init__()
        self._last_emission = None
        self._cache = None

    def configure(self, context):
        super().configure(context)
        self._last_emission = self.persistence.load('last_emission')
        self._cache = self.persistence.load('cache') or []

    def stop(self):
        self._backup()
    
    def process_signals(self, signals):
        now = datetime.utcnow()
        self._cache.extend(signals)
        if self._last_emission is None or \
           now - self._last_emission > self.interval:
            self._logger.debug(
                "Emitting %d signals at %s" % (len(self._cache), now))
                
            self._last_emission = now
            self.notify_signals(self._cache)
            
            # reset the cache and persist state data
            self._cache = []
            self.persistence.store('last_emission', now)
            self._backup()

    def _backup(self):
        self.persistence.store('cache', self._cache)
        self.persistence.save()
        

        
