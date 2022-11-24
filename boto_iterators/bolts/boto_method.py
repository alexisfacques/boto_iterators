"""A generic iterator bolt wrapping a given boto service method."""
import logging
from types import MethodType
from typing import Any, Callable, Dict, Iterator, Optional, Union

# From requirements.txt:
from boto3.session import Session

# From local modules:
from ..type_hints import BoltItem, IteratorBolt, Result
from ..utils import as_iterator, downcase_dict_keys


CAMELCASED_SERVICES = ('batch',)
LOGGER = logging.getLogger()


def boto_method(ServiceName: str, MethodName: str,
                Then: Optional[Callable[[Result], Union[Result, Iterator[Result]]]] = None, **boto_kwargs):
    """
    Configure an IteratorBolt factory to wrap a given boto service method.

    :param ServiceName: name of the low level boto client.
    :param MethodName: name of the client boto method to make a bolt from.
    :param IteratingOver: name of the method argument(s) that are expected to be iterated over.
    :param Then: an optional mapping function to handle a method object, or paginator page.

    :return: an iterator bolt factory.
    """
    def factory(**method_kwargs) -> IteratorBolt:
        """
        Configure an IteratorBolt wrapping a given boto service method.

        :param method_kwargs: extra fixed keyword arguments (not originating from the iteration) of the boto method.

        :return: a IteratorBolt function wrapping the boto client.
        """
        def bolt(BoltItems: Iterator[BoltItem], BotoSession: Session = Session()) -> Iterator[BoltItem]:
            """
            Iterate over BoltItems and execute a given boto service method with a given boto Session.

            :param BoltItems: iteration items, representing the the arguments values that are iterable, and results
                              from the previous bolts (accessible with the transform bolt).
            :param BotoSession: an optional boto Session.

            :return: the result of the boto operation.
            """
            items_count: int = 0

            for item, prev in BoltItems:
                client = BotoSession.client(ServiceName, **boto_kwargs)  # type: ignore

                if not (hasattr(client, MethodName)
                        and isinstance(getattr(client, MethodName), MethodType)):
                    raise ValueError('Boto client method \'%s.%s\' does not exist.'
                                     % (ServiceName, MethodName))

                try:
                    kwargs = {**method_kwargs, **item}

                    if ServiceName in CAMELCASED_SERVICES:
                        kwargs = downcase_dict_keys(kwargs)

                    res = getattr(client, MethodName)(**kwargs)

                except Exception as err:  # pylint: disable=broad-except
                    LOGGER.error('An unhandled exception has occured executing boto method \'%s.%s\'.',
                                 ServiceName, MethodName,
                                 extra={'methodKwargs': kwargs,
                                        'error': type(err).__name__,
                                        'errorDetail': str(err)})

                else:
                    for ret_item in as_iterator(res if Then is None else Then(res)):
                        items_count += 1
                        ret: Dict[str, Any] = {**kwargs, **ret_item}
                        yield ret, (*prev, ret)

            LOGGER.info('Successfully \'%s\' %d item(s) in %s.', MethodName, items_count, ServiceName)

        return bolt

    return factory
