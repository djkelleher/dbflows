from uuid import uuid4

from dbgress.load import create_row_id_generator
from quicklogs import get_logger

logger = get_logger(__name__, stdout=True)

def test_create_id(key_multi_value_table):
    create_id = create_row_id_generator(key_multi_value_table, row_id_column="key", logger=logger)
    _id = create_id({"key": str(uuid4()), "data1": str(uuid4()), "data2": str(uuid4())})
    assert isinstance(_id, str)
    assert len(_id)


def test_same_id(key_multi_value_table):
    """Test that the same data creates the same ID."""
    create_id = create_row_id_generator(key_multi_value_table, row_id_column="key", logger=logger)
    row = {"key": str(uuid4()), "data1": str(uuid4()), "data2": str(uuid4())}
    id1 = create_id(row)
    id2 = create_id(row)
    assert id1 == id2


def test_different_id(key_multi_value_table):
    """Check that different data creates a different ID."""
    create_id = create_row_id_generator(key_multi_value_table, row_id_column="key", logger=logger)
    row = {"key": str(uuid4()), "data1": str(uuid4()), "data2": str(uuid4())}
    id1 = create_id(row)
    row = {"key": str(uuid4()), "data1": str(uuid4()), "data2": str(uuid4())}
    id2 = create_id(row)
    assert id1 != id2
