from datetime import datetime
from nio.common.block.base import Block
from nio.common.discovery import Discoverable, DiscoverableType
from nio.metadata.properties.bool import BoolProperty
from nio.metadata.properties.timedelta import TimeDeltaProperty
from nio.metadata.properties.string import StringProperty
from nio.modules.scheduler import Job
from nio.common.signal.base import Signal


@Discoverable(DiscoverableType.block)
class Buffer(Block):

    interval = TimeDeltaProperty(title='Buffer Interval')
    timeout = BoolProperty(title='Buffer Timeout', default=False)
    timeout_attr = StringProperty(title='Timeout Attributes', visible=False, default="timeout")
    use_persistence = BoolProperty(title='Use Persistence?', visible=False, default=False)

    def __init__(self):
        super().__init__()
        self._last_emission = None
        self._cache = []
        self._emission_job = None

    def configure(self, context):
        super().configure(context)
        if self.use_persistence:
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
        if self.use_persistence:
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
        signals = self._cache
        self._cache = []
        if signals:
            self.notify_signals(signals)
        elif self.timeout:
            self.notify_signals([Signal({self.timeout_attr: True})])
        if self.use_persistence:
            self.persistence.store('last_emission', self._last_emission)
            self._backup()

    def process_signals(self, signals):
        self._cache.extend(signals)

    def _backup(self):
        self.persistence.store('cache', self._cache)
        self.persistence.save()
