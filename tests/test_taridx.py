import os
import glob
import taridx
from util import dummy_content

import pytest


@pytest.fixture()
def setup_files():
    dummy_content.create_dummy_files()
    dummy_content.create_archive()
    yield
    dummy_content.remove_dummy_files()
    dummy_content.remove_archive()




def test_taridx(setup_files):
    # build index
    with taridx.open(dummy_content.TARFILE) as tar:
        fs = glob.glob(os.path.join(dummy_content.TEST_DIR, '*'))
        for f in fs:
            assert open(f, 'r').read() == tar.extractfile(f).read().decode('utf-8')

    # delete index
    os.remove(tar.indexfile)
