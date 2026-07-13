class UnknownCommandError(Exception):
    """
    Тип команды не поддерживается складом. Невосстановимо: ретрай даст тот же
    результат, поэтому сообщение уходит в DLQ и ACK'ается - иначе оно отравит
    партицию бесконечным NACK-циклом.
    """

    def __init__(self, command_type: str) -> None:
        self.command_type = command_type
        super().__init__(f"unknown command type: '{command_type}'")
