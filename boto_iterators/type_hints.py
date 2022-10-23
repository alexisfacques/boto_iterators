"""Type hints for iterator bolts."""
from typing import Any, Callable, Dict, Iterator, Tuple

# From requirements.txt:
import boto3

Result = Dict[str, Any]
PreviousResults = Tuple[Dict[str, Any], ...]
BoltItem = Tuple[Result, PreviousResults]

IteratorBolt = Callable[[Iterator[BoltItem], boto3.session.Session], Iterator[BoltItem]]
