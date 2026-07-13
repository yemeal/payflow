from app.domain.exceptions.base import AppError


class ConcurrentCommandError(AppError):
    """
    Ту же команду в тот же момент обрабатывает другой консюмер: журнал
    идемпотентности уже занят этим command_id.

    Технический (восстановимый) сбой: транзакция откатывается, NACK_ON_ERROR
    переигрывает команду, на второй попытке срабатывает дедуп и переиздаётся
    сохранённый результат. Гасить исключение здесь нельзя - иначе бизнес-эффект
    закоммитится дважды.
    """

    def __init__(self, command_id: str) -> None:
        super().__init__(
            message=f"command {command_id} is being processed concurrently",
            details=command_id,
        )
        self.command_id = command_id
