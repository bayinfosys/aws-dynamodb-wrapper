"""Model registry for checking all models against objects

Usage:
```python
ModelRegistry.register(Story)
ModelRegistry.register(UserSignup)

item = ModelRegistry.from_stream(record)
```
"""
class ModelRegistry:
    models = []

    @classmethod
    def register(cls, model_cls):
        cls.models.append(model_cls)

    @classmethod
    def from_stream(cls, record):
        for model in cls.models:
            try:
                return model.from_stream_record(record)
            except ValueError:
                continue
        raise ValueError("No model matched the stream record")


def register_model(cls):
    """registor model decorator
    """
    ModelRegistry.register(cls)
    return cls
