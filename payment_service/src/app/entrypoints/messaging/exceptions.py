"""Исключения командного консьюмера (топик payments.commands)."""


class CommandConsumerError(Exception):
    """База для ошибок обработки команд."""


class UnknownCommandError(CommandConsumerError):
    """
    Для типа команды не зарегистрирован обработчик.

    Невосстановимо: повтор не поможет, команда уходит в DLQ.
    Раньше такая команда молча возвращала None и терялась (ACK без обработки).
    """

    def __init__(self, command_type: str) -> None:
        self.command_type = command_type
        super().__init__(
            f"Не зарегистрирован обработчик для команды '{command_type}'"
        )
