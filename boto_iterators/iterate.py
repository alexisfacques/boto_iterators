# mypy: ignore-errors
"""A lambda function handler wrapper to iterate over ESM records as if they were single events."""
from functools import wraps
import json
import logging
from typing import Any, Callable, Dict, Optional, Iterator, Tuple

# From local modules:
from .type_hints import IteratorChain, BoltItem


LOGGER = logging.getLogger()


def iterate():
    """Iterate over ESM records as if they were single events."""
    def lambda_handler(iterator_chain_fn: IteratorChain) -> Callable[[Dict[str, Any], Optional[Any]],
                                                                     Tuple[BoltItem, ...]]:
        """
        Execute lambda handler function as an iteration of ESM records ran through an iterator chain.

        :param iterator_chain_fn: the iterator bolts to execute.

        :return: the result of the iteration.
        """
        @wraps(iterator_chain_fn)
        def wrapper(event: Dict[str, Any], context: Optional[Any] = None) -> Tuple[BoltItem, ...]:
            """
            Lambda function handler wrapper function.

            :param event: the lambda event.
            :param context: the lambda context, if any.

            :return: the result of the iterator chain function.
            """
            # Not an SQS event...
            if 'Records' not in event or not isinstance(event['Records'], list):
                LOGGER.debug('Event is not an ESM record. Continuing...')
                generator: Iterator[BoltItem] = iter((event,))

            else:
                LOGGER.debug('Event is an ESM record. Iterating over Record body.')

                generator = (json.loads(record['body']) if 'body' in record else record for record in event['Records'])

            args_count: int = iterator_chain_fn.__code__.co_argcount

            for bolt in (iterator_chain_fn() if args_count == 0
                         else iterator_chain_fn(event) if args_count == 1
                         else iterator_chain_fn(event, context)):
                generator = bolt(generator)

            LOGGER.debug('Starting iteration...')

            ret = tuple(generator)

            LOGGER.debug('Finished iteration.', extra={'iterateBoltItems': ret})

            return ret

        return wrapper

    return lambda_handler
