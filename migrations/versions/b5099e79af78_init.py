"""init

Revision ID: b5099e79af78
Revises: fb3c79908d07
Create Date: 2024-02-24 13:38:36.253784

"""

import sqlalchemy as sa
from alembic import op

from db import postgres


# revision identifiers, used by Alembic.
revision = "b5099e79af78"
down_revision = "fb3c79908d07"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_constraint("unique_title", "report_group", type_="unique")
    op.create_unique_constraint(
        "unique_title", "report_group", ["title", "report_name_id"]
    )
    # ### end Alembic commands ###


def downgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_constraint("unique_title", "report_group", type_="unique")
    op.create_unique_constraint("unique_title", "report_group", ["title", "parent_id"])
    # ### end Alembic commands ###
