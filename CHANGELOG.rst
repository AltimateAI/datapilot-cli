
Changelog
=========

0.3.1 (2026-07-01)
------------------

* Relax ``mcp`` dependency pin from ``~=1.9.0`` to ``>=1.9.0,<2.0.0`` so
  downstream consumers can use newer ``mcp`` releases. Only the stable
  client API (``ClientSession``, ``StdioServerParameters``, ``stdio_client``)
  is used, which is unchanged across ``mcp`` 1.x.

0.0.0 (2024-01-25)
------------------

* First release on PyPI.
