from typing_extensions import Literal

Decoder = Literal['bytes', 'BytesIO', 'json', 'text']
Method = Literal['delete', 'get', 'post', 'put']
