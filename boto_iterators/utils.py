"""Common utility functions used by multiple bolts."""
import logging
from typing import Any, Dict, Iterator


LOGGER = logging.getLogger()


def as_iterator(obj: Any) -> Iterator[Any]:
    """
    Escape strings and dicts to lists of one element. Return a safe, iterable object.

    :param obj: the object to safely make iterable.

    :return: an iterable object.
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
