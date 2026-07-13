"""add sliding auth sessions

Revision ID: 7a9c31f4d2e8
Revises: c4bbf6120b7e
Create Date: 2026-07-24

"""

from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa


revision: str = "7a9c31f4d2e8"
down_revision: str | None = "c4bbf6120b7e"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """
    Вынести срок бездействия в сессию и оставить refresh-токен одноразовым.

    Каждая старая запись становится отдельной сессией. Это сохраняет владельца,
    прежний дедлайн и состояние отзыва без знания открытого значения токена.
    """
    op.create_table(
        "auth_sessions",
        sa.Column("user_id", sa.Uuid(), nullable=False),
        sa.Column("idle_expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("revoked_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(
            ["user_id"],
            ["users.id"],
            name="fk_auth_sessions_user_id_users",
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_auth_sessions_user_id",
        "auth_sessions",
        ["user_id"],
        unique=False,
    )

    op.execute(
        """
        INSERT INTO auth_sessions (
            id,
            user_id,
            idle_expires_at,
            revoked_at,
            created_at
        )
        SELECT
            id,
            user_id,
            expires_at AT TIME ZONE 'UTC',
            CASE
                WHEN revoked
                THEN COALESCE(updated_at, created_at) AT TIME ZONE 'UTC'
                ELSE NULL
            END,
            created_at AT TIME ZONE 'UTC'
        FROM refresh_tokens
        """
    )

    op.add_column(
        "refresh_tokens",
        sa.Column("session_id", sa.Uuid(), nullable=True),
    )
    op.add_column(
        "refresh_tokens",
        sa.Column("used_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.execute(
        """
        UPDATE refresh_tokens
        SET
            session_id = id,
            used_at = CASE
                WHEN revoked
                THEN COALESCE(updated_at, created_at) AT TIME ZONE 'UTC'
                ELSE NULL
            END
        """
    )
    op.alter_column("refresh_tokens", "session_id", nullable=False)
    op.create_foreign_key(
        "fk_refresh_tokens_session_id_auth_sessions",
        "refresh_tokens",
        "auth_sessions",
        ["session_id"],
        ["id"],
        ondelete="CASCADE",
    )
    op.create_index(
        "ix_refresh_tokens_session_id",
        "refresh_tokens",
        ["session_id"],
        unique=False,
    )

    op.drop_constraint(
        "refresh_tokens_user_id_fkey",
        "refresh_tokens",
        type_="foreignkey",
    )
    op.drop_index("ix_refresh_tokens_user_id", table_name="refresh_tokens")
    op.drop_column("refresh_tokens", "user_id")
    op.drop_column("refresh_tokens", "revoked")
    op.drop_column("refresh_tokens", "expires_at")
    op.drop_column("refresh_tokens", "updated_at")


def downgrade() -> None:
    """Вернуть прежний контракт, приблизив состояние цепочки к revoked."""
    op.add_column(
        "refresh_tokens",
        sa.Column("user_id", sa.Uuid(), nullable=True),
    )
    op.add_column(
        "refresh_tokens",
        sa.Column(
            "revoked",
            sa.Boolean(),
            server_default=sa.false(),
            nullable=False,
        ),
    )
    op.add_column(
        "refresh_tokens",
        sa.Column("expires_at", sa.DateTime(), nullable=True),
    )
    op.add_column(
        "refresh_tokens",
        sa.Column("updated_at", sa.DateTime(), nullable=True),
    )

    op.execute(
        """
        UPDATE refresh_tokens AS token
        SET
            user_id = session.user_id,
            revoked = (
                token.used_at IS NOT NULL
                OR session.revoked_at IS NOT NULL
            ),
            expires_at = session.idle_expires_at AT TIME ZONE 'UTC',
            updated_at = COALESCE(
                token.used_at,
                session.revoked_at
            ) AT TIME ZONE 'UTC'
        FROM auth_sessions AS session
        WHERE token.session_id = session.id
        """
    )
    op.alter_column("refresh_tokens", "user_id", nullable=False)
    op.alter_column("refresh_tokens", "expires_at", nullable=False)
    op.create_foreign_key(
        "refresh_tokens_user_id_fkey",
        "refresh_tokens",
        "users",
        ["user_id"],
        ["id"],
        ondelete="CASCADE",
    )
    op.create_index(
        "ix_refresh_tokens_user_id",
        "refresh_tokens",
        ["user_id"],
        unique=False,
    )

    op.drop_index("ix_refresh_tokens_session_id", table_name="refresh_tokens")
    op.drop_constraint(
        "fk_refresh_tokens_session_id_auth_sessions",
        "refresh_tokens",
        type_="foreignkey",
    )
    op.drop_column("refresh_tokens", "used_at")
    op.drop_column("refresh_tokens", "session_id")

    op.drop_index("ix_auth_sessions_user_id", table_name="auth_sessions")
    op.drop_table("auth_sessions")
