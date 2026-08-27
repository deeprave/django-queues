# Copilot instructions

## Exception handlers (PEP 758)

This project requires Python 3.14 or later. [PEP 758](https://peps.python.org/pep-0758/), implemented in Python 3.14, allows `except` and `except*` to list multiple exception types without parentheses:

```python
except AttributeError, TypeError, ValueError:
```

That is valid syntax here. `ruff format` emits this form on purpose for the 3.14 target. Do not report it as Python 2 `except E, e` syntax, and do not report it as a `SyntaxError`. Do not rewrite it to `except (A, B, C):` — the formatter will strip the parentheses again.
