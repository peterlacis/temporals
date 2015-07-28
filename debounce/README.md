Debouncer
=======

For each group in **group_by**, after an input signal is passed through, all succeeding signals of the same group will be grouped for **interval** amount of time.

Properties
--------------

-   **interval**: Amount of time to wait before allowing another signal in a matching group.
-   **group_by**: Expression proprety. The value by which signals are grouped.


Dependencies
----------------
[GroupBy Block Supplement](https://github.com/nio-blocks/block_supplements/tree/master/group_by)

Commands
----------------
None

Input
-------
Any list of signals.

Output
---------
The first signal for each group in **group_by**, every **interval**.
