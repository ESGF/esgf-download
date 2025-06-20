"""add_dataset_tracking

Revision ID: e7edab5d4e4b
Revises: 0.8.0
Create Date: 2025-05-23 17:38:22.066153

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'e7edab5d4e4b'
down_revision = '0.8.0'
branch_labels = None
depends_on = None


def upgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table('dataset',
    sa.Column('dataset_id', sa.String(length=255), nullable=False),
    sa.Column('total_files', sa.Integer(), nullable=False),
    sa.Column('created_at', sa.DateTime(), server_default=sa.text('(CURRENT_TIMESTAMP)'), nullable=False),
    sa.Column('updated_at', sa.DateTime(), server_default=sa.text('(CURRENT_TIMESTAMP)'), nullable=False),
    sa.PrimaryKeyConstraint('dataset_id')
    )
    with op.batch_alter_table('file', schema=None) as batch_op:
        batch_op.create_foreign_key('fk_file_dataset', 'dataset', ['dataset_id'], ['dataset_id'])

    # ### end Alembic commands ###
    
def downgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    with op.batch_alter_table('file', schema=None) as batch_op:
        batch_op.drop_constraint('fk_file_dataset', type_='foreignkey')

    op.drop_table('dataset')
    # ### end Alembic commands ###
