"""Common utility functions used by multiple bolts."""
import logging
from typing import Any, Dict, Iterator, Union, Tuple


LOGGER = logging.getLogger()


def as_iterator(obj: Any) -> Iterator[Any]:
    """
    Escape strings and dicts to lists of one element. Return a safe, iterable object.

    :param obj: the object to safely make iterable.

    :return:    an iterable object.
    """
    if isinstance(obj, Iterator):  # pylint: disable=isinstance-second-argument-not-valid-type
        return obj

    return iter((obj,))


def downcase_dict_keys(d: Dict[str, Any]):
    """
    Downcase the first letter of every key in a dictionnary because for some reasons, some services use camelcasing.

    :param d: the dictionnary.

    :return: the resulting dictionnary.
    """
    return {k[0].lower() + k[1:]: downcase_dict_keys(v) if isinstance(v, dict)
            else v for k, v in d.items()}


def get_boto_method_kwargs_from_entries(entries: Union[str, Tuple[str, ...]], item) -> Dict[str, Any]:
    """
    Handle various scenarios of IteratingOver entries configuration.

    :param entries: the IteratingOver keys configured for a given boto (method/paginator) bolt.
    :return: the boto method kwargs
    """
    if isinstance(entries, str):
        return {entries: item[entries]} if isinstance(item, dict) and entries in item else {entries: item}

    if isinstance(item, dict):
        return {k: item[k] for k in entries if k in item}

    if isinstance(item, (list, tuple)):
        if len(item) != len(entries):
            LOGGER.error('Expected %d argument(s), got %d.', len(entries), len(item),
                         extra={'item': item})
            raise TypeError('Expected %d argument(s), got %d.' % (len(entries), len(item)))

        return {k: item[idx] for idx, k in enumerate(entries)}

    LOGGER.error('Unsupported item return type.', extra={'item': item, 'itemType': type(item).__name__})

    raise TypeError('Unsupported item return type.')
