from functools import wraps
import json
import logging
from typing import Any, Dict, Optional, Iterator, Tuple

# From requirements.txt:

LOGGER = logging.getLogger()


def iterate():
    """
    """
    def lambda_handler(iterator_chain):
        """
        Wrap up a decorated function.

        :params lambda_handler: the decorated function.

        :return: the decorated function result.
        """
        @wraps(iterator_chain)
        def wrapper(event: Dict[str, Any], context: Optional[Any] = None, *args, **kwargs) -> Tuple[Any, ...]:
            """
            """
            prev: Tuple[Any, ...] = tuple()

            def resolve(idx: int) -> Dict[str, Any]:
                return prev[idx]

            # Not an SQS event...
            if 'Records' not in event or \
                    not isinstance(event['Records'], list):
                LOGGER.debug('Event is not an ESM record. Continuing...')
                generator: Iterator[Any] = iter(((event, prev),))

            else:
                LOGGER.debug('Event is an ESM record. Iterating over Record body.')

                generator = (((json.loads(record['body']) if 'body'
                               in record else record), prev)
                             for record in event['Records'])

            args_count: int = iterator_chain.__code__.co_argcount

            for bolt in (iterator_chain() if args_count == 0
                         else iterator_chain(event) if args_count == 1
                         else iterator_chain(event, context) if args_count == 2
                         else iterator_chain(event, context, resolve)
                         if args_count == 3
                         else iterator_chain(event, context, resolve, *args,
                                             **kwargs)):
                generator = bolt(generator)

            LOGGER.debug('Starting iteration...')

            ret = tuple(res for res, prev in generator)

            LOGGER.debug('Finished iteration.', extra={'iterateResults': ret})

            return ret

        return wrapper

    return lambda_handler
