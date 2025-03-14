"""init

Revision ID: cba87c2c864c
Revises:
Create Date: 2024-02-23 18:31:12.231025

"""

import sqlalchemy as sa
from alembic import op

from db import postgres


# revision identifiers, used by Alembic.
revision = "cba87c2c864c"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table(
        "report_name",
        sa.Column("id", sa.UUID(), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=True),
        sa.Column("updated_at", sa.DateTime(), nullable=True),
        sa.Column("title", sa.String(length=255), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("id"),
        sa.UniqueConstraint("title"),
    )
    op.create_table(
        "report_group",
        sa.Column("parent_id", sa.UUID(), nullable=True),
        sa.Column("report_name_id", sa.UUID(), nullable=False),
        sa.Column("id", sa.UUID(), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=True),
        sa.Column("updated_at", sa.DateTime(), nullable=True),
        sa.Column("title", sa.String(length=255), nullable=False),
        sa.ForeignKeyConstraint(
            ["report_name_id"], ["report_name.id"], ondelete="CASCADE"
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("id"),
        sa.UniqueConstraint("title"),
    )
    op.create_table(
        "report_element",
        sa.Column("group_id", sa.UUID(), nullable=False),
        sa.Column("id", sa.UUID(), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=True),
        sa.Column("updated_at", sa.DateTime(), nullable=True),
        sa.Column("title", sa.String(length=255), nullable=False),
        sa.ForeignKeyConstraint(["group_id"], ["report_group.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("id"),
        sa.UniqueConstraint("title"),
    )
    # ### end Alembic commands ###


def downgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_table("report_element")
    op.drop_table("report_group")
    op.drop_table("report_name")
    # ### end Alembic commands ###
