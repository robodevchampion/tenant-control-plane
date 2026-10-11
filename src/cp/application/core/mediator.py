import inspect
from typing import Any

from injector import Injector


class Mediator:
    handlers: dict[type, type] = {}
    injector: Injector = Injector()

    async def send(self, entity: object, **context: Any) -> Any:
        try:
            handler_type = self.handlers[type(entity)]
        except KeyError as exc:
            raise LookupError(f"No handler registered for {type(entity).__name__}") from exc
        handler = self.injector.get(handler_type)
        result = handler.handle(entity, **context)
        return await result if inspect.isawaitable(result) else result

    async def send_all(self, entities: list[object], **context: Any) -> list[Any]:
        return [await self.send(entity, **context) for entity in entities]

    @classmethod
    def register_handler(cls, key: type):
        def decorator(handler_type: type) -> type:
            cls.handlers[key] = handler_type
            return handler_type

        return decorator
