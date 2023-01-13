"""A generic bolt to execute an IteratorChain using a specific boto Session."""
import logging
from typing import Iterator, Tuple

# From requirements.txt:
import boto3

# From local modules:
from .boto_method import boto_method
from ..type_hints import BoltItem, IteratorBolt


LOGGER = logging.getLogger()


def boto_assume_role(IteratorChain: Tuple[IteratorBolt, ...], **method_kwargs) -> IteratorBolt:
    """
    Configure an IteratorBolt assuming a role session based of a role arn, and execute an iterator chain.

    :param IteratorChain: an ordered list of iterator bolts to execute with the session role arn.
    :param method_kwargs: extra sts.assume_role method kwargs.

    :return: an IteratorBolt.
    """
    def execute_iterator_bolts(CredentialsBoltItem: BoltItem) -> Iterator[BoltItem]:
        """
        :param CredentialsBoltItem: the result of the sts.assume_role method.

        :return: the result of execution of the IteratorChain, having used the boto session.
        """
        nested_session: boto3.session.Session = boto3.session.Session(**{
            'aws_access_key_id': CredentialsBoltItem['Credentials']['AccessKeyId'],
            'aws_secret_access_key': CredentialsBoltItem['Credentials']['SecretAccessKey'],
            'aws_session_token': CredentialsBoltItem['Credentials']['SessionToken']
        })

        generator: Iterator[BoltItem] = iter((CredentialsBoltItem,))

        LOGGER.debug('Assembling \'boto.assume_role\' iterator chain.')

        for bolt in IteratorChain:
            generator = bolt(generator, nested_session)

        return generator

    return boto_method(ServiceName='sts', MethodName='assume_role', IteratingOver='RoleArn',
                       Then=execute_iterator_bolts)(**method_kwargs)
