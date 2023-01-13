"""Just some wrapper code to chain an iterator bolt with the rest of a chain execution."""
import logging
from typing import Callable, Iterator, Union

# From requirements.txt:
from boto3.session import Session

# From local modules:
from ..type_hints import BoltItem, IteratorBolt


LOGGER = logging.getLogger()


def then(Parser: Union[Callable[[Iterator[BoltItem]], Iterator[BoltItem]],
                       Callable[[Iterator[BoltItem], Session], Iterator[BoltItem]]]) -> IteratorBolt:
    """
    """
    def bolt(BoltItems: Iterator[BoltItem], BotoSession: Session = Session()) -> Iterator[BoltItem]:
        """
        """
        return (Parser(BoltItems) if Parser.__code__.co_argcount == 1  # type: ignore
                else Parser(BoltItems, BotoSession))  # type: ignore

    return bolt
