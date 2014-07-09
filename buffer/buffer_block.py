from datetime import datetime
from nio.common.block.base import Block
from nio.common.discovery import Discoverable, DiscoverableType
from nio.metadata.properties.timedelta import TimeDeltaProperty
from nio.modules.scheduler.imports import Job


@Discoverable(DiscoverableType.block)
class Buffer(Block):

    interval = TimeDeltaProperty()

    def __init__(self):
        super().__init__()
        self._last_emission = None
        self._cache = []
        self._emission_job = None

    def configure(self, context):
        super().configure(context)
        self._last_emission = self.persistence.load('last_emission')
        self._cache = self.persistence.load('cache') or []

    def start(self):
        now = datetime.utcnow()
        latest = self._last_emission or now
        delta = self.interval - (now - latest)
        self._emission_job = Job(
            self.emit,
            delta,
            False,
            reset=True
        )

    def stop(self):
        self._backup()

    def emit(self, reset=False):
        if reset:
            self._emission_job.cancel()
            self._emission_job = Job(
                self.emit,
                self.interval,
                True
            )
        self._last_emission = datetime.utcnow()
        self.persistence.store('last_emission', self._last_emission)
        signals = self._cache
        self._cache = []
        self.notify_signals(signals)
        self._backup()
    
    def process_signals(self, signals):
        self._cache.extend(signals)
        
    def _backup(self):
        self.persistence.store('cache', self._cache)
        self.persistence.save()
        

        