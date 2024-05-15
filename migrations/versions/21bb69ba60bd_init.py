"""init

Revision ID: 21bb69ba60bd
Revises: 39c548457c1a
Create Date: 2024-02-29 08:48:31.808728

"""

import sqlalchemy as sa
from alembic import op

from db import postgres


# revision identifiers, used by Alembic.
revision = "21bb69ba60bd"
down_revision = "39c548457c1a"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_constraint(
        "google_report_ids_month_key", "google_report_ids", type_="unique"
    )
    op.create_unique_constraint(
        "unique_report_type_in_month", "google_report_ids", ["month", "report_type"]
    )
    # ### end Alembic commands ###


def downgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_constraint(
        "unique_report_type_in_month", "google_report_ids", type_="unique"
    )
    op.create_unique_constraint(
        "google_report_ids_month_key", "google_report_ids", ["month"]
    )
    # ### end Alembic commands ###
