"""modify_google_report_table

Revision ID: 0ce804d34c21
Revises: 6fc88f24054f
Create Date: 2024-02-27 19:14:20.442180

"""

import sqlalchemy as sa
from alembic import op

from db import postgres


# revision identifiers, used by Alembic.
revision = "0ce804d34c21"
down_revision = "6fc88f24054f"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column(
        "google_report_ids",
        sa.Column("report_type", sa.String(length=255), nullable=False),
    )
    op.create_unique_constraint(None, "google_report_ids", ["id"])
    # ### end Alembic commands ###


def downgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_constraint(None, "google_report_ids", type_="unique")
    op.drop_column("google_report_ids", "report_type")
    # ### end Alembic commands ###
