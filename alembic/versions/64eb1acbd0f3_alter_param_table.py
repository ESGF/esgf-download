"""alter param table

Revision ID: 64eb1acbd0f3
Revises: 08c857708e92
Create Date: 2022-07-11 12:03:42.020862

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "64eb1acbd0f3"
down_revision = "08c857708e92"
branch_labels = None
depends_on = None


def upgrade() -> None:
    with op.batch_alter_table(
        "param",
        recreate="always",
        partial_reordering=[("id", "name", "value", "last_updated")],
    ) as bop:
        bop.add_column(sa.Column("id", sa.Integer, primary_key=True))
        bop.alter_column("name", type_=sa.String(50), nullable=False)
        bop.alter_column("value", type_=sa.String(255), nullable=False)
        bop.add_column(
            sa.Column(
                "last_updated",
                sa.DateTime(timezone=True),
                server_default=sa.func.now(),
                onupdate=sa.func.now(),
            )
        )
        bop.create_unique_constraint("param_name_value", ["name", "value"])


def downgrade() -> None:
    op.drop_table("param_new")
