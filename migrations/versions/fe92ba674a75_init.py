"""init

Revision ID: fe92ba674a75
Revises: 09c6cef03166
Create Date: 2024-02-28 18:02:24.673490

"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

from db import postgres


# revision identifiers, used by Alembic.
revision = "fe92ba674a75"
down_revision = "09c6cef03166"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table(
        "report_cache",
        sa.Column("date", sa.DATE(), nullable=False),
        sa.Column("report_type", sa.String(length=255), nullable=False),
        sa.Column(
            "report_data", postgresql.JSONB(astext_type=sa.Text()), nullable=False
        ),
        sa.Column("id", sa.UUID(), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=True),
        sa.Column("updated_at", sa.DateTime(), nullable=True),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("date", "report_type", name="unique_report"),
        sa.UniqueConstraint("id"),
    )
    # ### end Alembic commands ###


def downgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_table("report_cache")
    # ### end Alembic commands ###
