# PeerColab Engine - Python

Transport abstraction library for operation dispatching with interceptors.

## Installation

```bash
pip install peercolab-engine
```

## Quick Start

```python
from peercolab_engine import Transport, RequestOperation, Result

class GetUser(RequestOperation[dict, dict]):
    def __init__(self):
        super().__init__("users.get", "GET")

get_user = GetUser()

session = (
    Transport.session("my-service")
    .intercept(get_user.handle(my_handler))
    .build()
)
```
