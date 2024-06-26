"""init

Revision ID: 39c548457c1a
Revises: 2ffc1af2d2c2
Create Date: 2024-02-28 18:36:10.301490

"""

import sqlalchemy as sa
from alembic import op

from db import postgres


# revision identifiers, used by Alembic.
revision = "39c548457c1a"
down_revision = "2ffc1af2d2c2"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.alter_column(
        "report_cache",
        "report_date",
        existing_type=sa.DATE(),
        type_=sa.String(length=255),
        existing_nullable=False,
    )
    # ### end Alembic commands ###


def downgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.alter_column(
        "report_cache",
        "report_date",
        existing_type=sa.String(length=255),
        type_=sa.DATE(),
        existing_nullable=False,
    )
    # ### end Alembic commands ###
