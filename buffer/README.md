Buffer
=======

Collects all incoming signals and then emits them every **interval**.

If **timeout** is set *True* and no signals were input during an interval, a timeout signal will be output at the end of the interval. This siganl will have a single attribute, *timeout*, that is set to *True*.

If **use_persistence** is *True*, then persistence is used to maintain the list of signals between stopping and starting the block.

Properties
--------------

-   **interval**: Time interval at which signals are emitted.
-   **timeout** (False): If *True* then a timeout signal will be emitted at the end of any interval with no signals.
-   **timeout_attr** ('timeout'): When a timeout signal is emitted, an attribute with the value will be set to True on the signal.
-   **use_persistence** (False): If *True*, use persistence to store the list of signals.

Dependencies
----------------
None

Commands
----------------
None

Input
-------
Any list of signals.

Output
---------
At the end of every **interval** all input signals since the last **interval** will be emitted.
