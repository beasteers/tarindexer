import os
import glob
import tarfile as trf
from tqdm import tqdm
import taridx

TARFILE = 'tmp/tmp.tar'
TEST_DIR = 'tmp/data'

def get_content(fname, n=1000, L=100000):
    return '{fname}{content}{fname}'.format(fname=fname, content='abc' * n)

def create_dummy_files(n=500, L=1000000):
    # setup directory
    os.makedirs(TEST_DIR, exist_ok=True)
    print('generating {} files of scale {} in'.format(n, L), TEST_DIR)

    # get file names
    file_pat = os.path.join(TEST_DIR, 'file_{}.txt')
    fs = [file_pat.format(i) for i in range(n)]

    # create files
    for fname in tqdm(fs):
        with open(fname, 'w') as f:
            f.write(get_content(fname, L))

    print('generated', len(fs), 'files')
    return fs

def create_archive(gz=False):
    # create tar
    f = TARFILE + ('.gz' if gz else '')
    mode = "w|" + ('gz' if gz else '')
    with trf.open(f, mode) as tar:
        tar.add(TEST_DIR)
    print('wrote archive to', f)
    return f





def remove_dummy_files():
    # clean upppppp
    for f in glob.glob(os.path.join(TEST_DIR, '*')):
        os.remove(f)
    os.rmdir(TEST_DIR)
    print('deleting', TEST_DIR)

def remove_archive(gz=False):
    # remove tar
    f = TARFILE + ('.gz' if gz else '')
    os.remove(f)
    print('deleting', f)

def remove_index():
    f = taridx.to_indexfile(TARFILE)
    os.remove(f)
    print('deleting', f)
