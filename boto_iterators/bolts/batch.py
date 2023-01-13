"""A generic bolt to transform the output of other bolts."""
from itertools import islice
from typing import Iterator, Tuple

# From local modules:
from ..type_hints import BoltItem, IteratorBolt


def batch(BatchSize: int) -> IteratorBolt:
    """
    Configure a Chain bolt that yields the output of the 'Parser' function.

    Function factory.

    :param Parser: a function that takes the bolt input as argument,
                   and return a transformed result. This result is yield
                   by the Iterator Chain.

    :return: an Iterator Chain bolt reading anything and yielding the transformed result of 'parser'.
    """
    def bolt(BoltItems: Iterator[BoltItem], *_args, **_kwargs) -> Iterator[BoltItem]:
        """
        Transform all 'items' to the output of 'parser'.

        :param items: the items to transform.

        :return: an enumeration of the results of the 'parser' function.
        """
        while True:
            batched_records: Tuple[BoltItem, ...] = tuple(islice(BoltItems, BatchSize))
            if not batched_records:
                return
            yield {'Records': batched_records}

    return bolt
