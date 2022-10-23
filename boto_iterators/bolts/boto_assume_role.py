"""A generic bolt to execute an IteratorChain using a specific boto Session."""
import logging
from typing import Iterator, Tuple

# From requirements.txt:
import boto3

# From local modules:
from .boto_method import boto_method
from ..type_hints import BoltItem, IteratorBolt, Result


LOGGER = logging.getLogger()


def boto_assume_role(IteratorChain: Tuple[IteratorBolt, ...], **method_kwargs) -> IteratorBolt:
    """
    Configure an IteratorBolt assuming a role session based of a role arn, and execute an iterator chain.

    :param IteratorChain: an ordered list of iterator bolts to execute with the session role arn.
    :param method_kwargs: extra sts.assume_role method kwargs.

    :return: an IteratorBolt.
    """
    def execute_iterator_bolts(CredentialsResult: Result) -> Iterator[Result]:
        """
        :param CredentialsResult: the result of the sts.assume_role method.

        :return: the result of execution of the IteratorChain, having used the boto session.
        """
        nested_session: boto3.session.Session = boto3.session.Session(**{
            'aws_access_key_id': CredentialsResult['Credentials']['AccessKeyId'],
            'aws_secret_access_key': CredentialsResult['Credentials']['SecretAccessKey'],
            'aws_session_token': CredentialsResult['Credentials']['SessionToken']
        })

        generator: Iterator[BoltItem] = iter(((CredentialsResult, tuple()),))

        LOGGER.debug('Assembling \'boto.assume_role\' iterator chain.')

        for bolt in IteratorChain:
            generator = bolt(generator, nested_session)

        for item, _ in generator:
            yield item

    return boto_method(ServiceName='sts', MethodName='assume_role', IteratingOver='RoleArn',
                       Then=execute_iterator_bolts)(**method_kwargs)
