from dataset import Dataset
from pytest import fixture

from dataset import EXTERNAL_DATA_DIR

@fixture
def my_dataset():
    return Dataset()

def test_contributions_external_dir():
    assert EXTERNAL_DATA_DIR

