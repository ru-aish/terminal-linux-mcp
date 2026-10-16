"""Optional backend adapters.

The core gateway has no dependency on any specific ChatGPT client. Adapters in
this package are imported only when explicitly selected by an operator.
"""

from .codex_renderer import CodexRendererBackend
from .direct_chatgpt import DirectChatGPTBackend

__all__ = ["CodexRendererBackend", "DirectChatGPTBackend"]
