"""A generic bolt to transform the output of other bolts."""
import logging
from typing import Callable, Iterator, Optional, Union

# From requirements.txt:
from boto3.session import Session

# From local modules:
from ..utils import as_iterator
from ..type_hints import BoltItem, IteratorBolt, PreviousResults, Result


LOGGER = logging.getLogger()


def transform(Parser: Union[Callable[[Result], Optional[Union[Result, Iterator[Result]]]],
                            Callable[[Result, PreviousResults], Optional[Union[Result, Iterator[Result]]]],
                            Callable[[Result, PreviousResults, Session], Optional[Union[Result, Iterator[Result]]]]]) \
            -> IteratorBolt:
    """
    Configure a Chain bolt that yields the output of the 'Parser' function.

    Function factory.

    :param Parser: a function that takes the bolt input as argument,
                   and return a transformed result. This result is yield
                   by the Iterator Chain.

    :return: an Iterator Chain bolt reading anything and yielding the transformed result of 'parser'.
    """
    def bolt(BoltItems: Iterator[BoltItem], BotoSession: Session = Session()) -> Iterator[BoltItem]:
        """
        Transform all 'items' to the output of 'parser'.

        :param items: the items to transform.

        :return: an enumeration of the results of the 'parser' function.
        """
        for item, prev in BoltItems:
            try:
                res: Optional[Union[Result, Iterator[Result]]] = \
                    (
                        Parser(item) if Parser.__code__.co_argcount == 1  # type: ignore
                        else Parser(item, prev) if Parser.__code__.co_argcount == 2  # type: ignore
                        else Parser(item, prev, BotoSession)  # type: ignore
                    )

                for parsed_item in as_iterator(res):
                    if parsed_item is not None:
                        yield parsed_item, (*prev, parsed_item)

            except Exception as err:  # pylint: disable=broad-except
                LOGGER.exception('An unhandled exception occured within the transformation function.',
                                 extra={'error': type(err).__name__, 'errorDetail': str(err),
                                        'item': item})

                raise RuntimeError('An unhandled exception occured within the transformation function.') from err

    return bolt
