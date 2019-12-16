import os
import json
import time
import taridx
import tarfile as trf
from util import dummy_content as dc
from util.timer import Timer
from tqdm import tqdm

import random

batch = lambda it, n=1: (
    it[i:i+n] for i in
    tqdm(range(0, len(it), n), total=len(it) / n))


# copy over util method for resetting member cache
trf.TarFile.clear_members = taridx.IndexedTarFile.clear_members

def compare(n=1000, L=5000):
    '''
    .tar vs.tar.gz vs .tar.index

    '''
    t0 = time.time()
    # create some test data
    fs = dc.create_dummy_files(n=n, L=L) # n=50000, L=20
    tarf = dc.create_archive()
    targzf = dc.create_archive(gz=True)

    # check that the index actually has entries in it
    with taridx.open(tarf) as tar:
        print(tar.ixtable.count(), list(tar.ixtable.find(_limit=3)))

    # define the tar variants to test
    creators = {
        '.tar': lambda: trf.open(tarf, 'r'),
        '.tar.gz': lambda: trf.open(targzf, 'r:gz'),
        '.tar.index': lambda: taridx.open(tarf, 'r:'),
    }

    # measure timing
    timing = {
        name: run_file_tests(create, fs, name).results()
        for name, create in creators.items()
    }

    # get file sizes
    tarsize = os.path.getsize(tarf)
    idxsize = os.path.getsize(taridx.to_indexfile(tarf))
    targzsize = os.path.getsize(targzf)
    timing['.tar']['filesize'] = '{:.2f}{}'.format(*taridx.humansize(tarsize))
    timing['.tar.gz']['filesize'] = '{:.2f}{}'.format(*taridx.humansize(targzsize))
    timing['.tar.index']['filesize'] = '{:.2f}{}'.format(*taridx.humansize(tarsize + idxsize))

    timing['total_time'] = time.time() - t0

    # save results
    os.makedirs('results', exist_ok=True)
    results_file = 'results/timing_results-n_{}-L_{}.json'.format(n, L)
    with open(results_file, 'w') as f:
        json.dump(timing, f)
    print('wrote results to', results_file)

    print(json.dumps(timing, indent=4))

    # cleanup
    dc.remove_dummy_files()
    dc.remove_archive()
    dc.remove_archive(gz=True)
    dc.remove_index()

    print('Took {:.1f}s in total.'.format(timing['total_time']))


def run_file_tests(create, fs, name):
    print('Running tests for:', name)
    timer = Timer()

    random.shuffle(fs)
    for fs_ in batch(fs, 500):
        # measure the time to open. compressed files take longer.
        timer.tick('open')
        with create() as tar:
            timer.tock('open')

            for f in fs_:
                tar.clear_members()

                # the first time it goes thru the entire tar file
                with timer.ticktock('get_member'):
                    m = tar.getmember(f)

                # the second time it should  already have them all gathered
                with timer.ticktock('get_member_2nd_time'):
                    m = tar.getmember(f)

                # check the speed when using an existing member object
                # (it already has offsets)
                with timer.ticktock('get_file_from_member'):
                    buf = tar.extractfile(m)

                # clear the member list (reset position to 0), and try
                # extracting file using filename.
                tar.clear_members()
                with timer.ticktock('get_file_from_filename'):
                    buf = tar.extractfile(f)

    print(json.dumps(timer.results(), indent=4))
    return timer


if __name__ == '__main__':
    sizes = [
        # basic test
        (10, 10),

        # up to scale
        (500, 500),
        # see the effect of file size
        (500, 1000),
        # see the effect of file count
        (1000, 500),

        # see the effect of both
        (1000, 1000),
        # even higher scale
        (1000, 10000),
        (10000, 1000),
        # much larger file sizes
        (1000, 100000),
    ]
    for n, L in sizes:
        compare(n, L)
