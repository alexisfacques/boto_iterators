"""A generic iterator bolt wrapping a given boto service paginator."""
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


def boto_paginator(ServiceName: str, MethodName: str,
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
        Configure an iterator bolt wrapping a given boto service method.

        :param method_kwargs: keyword arguments of the bolt boto method.

        :return: a iterator bolt function wrapping the boto client.
        """
        def wrapper(BoltItems: Iterator[BoltItem], BotoSession: Session = Session()) -> Iterator[BoltItem]:
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
                    raise ValueError('Boto client method \'%s.%s\' does not exist.' % (ServiceName, MethodName))

                try:
                    kwargs = {**method_kwargs, **item}

                    if ServiceName in CAMELCASED_SERVICES:
                        kwargs = downcase_dict_keys(kwargs)

                    for page in client.get_paginator(MethodName).paginate(**kwargs):
                        for page_item in as_iterator(page if Then is None else Then(page)):
                            items_count += 1
                            ret: Dict[str, Any] = {**kwargs, **page_item}
                            yield ret, (*prev, ret)

                except Exception as err:  # pylint: disable=broad-except
                    LOGGER.error('An unhandled exception has occured executing boto paginator \'%s.%s\'.',
                                 ServiceName, MethodName,
                                 extra={'methodKwargs': kwargs,
                                        'error': type(err).__name__,
                                        'errorDetail': str(err)})

            LOGGER.info('Successfully \'%s\' %d item(s) in %s.', MethodName, items_count, ServiceName)

        return wrapper

    return factory
