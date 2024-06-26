"""init

Revision ID: fb3c79908d07
Revises: 5d5fb5d75513
Create Date: 2024-02-24 13:37:31.583646

"""

import sqlalchemy as sa
from alembic import op

from db import postgres


# revision identifiers, used by Alembic.
revision = "fb3c79908d07"
down_revision = "5d5fb5d75513"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_unique_constraint("unique_title", "report_group", ["title", "parent_id"])
    # ### end Alembic commands ###


def downgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_constraint("unique_title", "report_group", type_="unique")
    # ### end Alembic commands ###
