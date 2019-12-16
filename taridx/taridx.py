#!/usr/bin/env python

import os
import io
import sqlite3
import dataset
import tarfile as trf
from contextlib import contextmanager

from tqdm import tqdm

# __all__ = ['IndexedTarFile']

class IndexedTarFile(trf.TarFile):
    '''Provides an interface for tar files with a file index (sqlite) for fast
    random access.

    NOTE: This does not work with compressed archives. It wouldn't give you much
    speed up anyways because most of the time is spent decompressing.

    Examples:
    >>> with taridx.open('my_archive.tar') as tar: # auto builds index if it doesn't exist.
    ...     # ****** FASTER ********
    ...     # will use the index instead of iterating though the entire archive
    ...     # to find a single file.
    ...     some_file = tar.getmember('some_file.txt')
    ...     some_file = tar.extractfile('some_file.txt')
    ...
    ...     # ****** SAME SPEED ********
    ...     # this still hops through the entire file because it still needs to
    ...     # extract the buffer in blocks so there's no speed up.
    ...     all_the_files = tar.getmembers()

    '''
    INDEX_TABLE = 'tarfile'

    def __init__(self, *a, indexfile=None, autoindex=True, attrs=None,
                 dbtype='sqlite:///', **kw):
        super().__init__(*a, **kw)

        # check for no compression modes
        p = max((self.mode.split(c) for c in '|:'), key=len)
        assert len(p) < 2 or p[1] == '*', 'cannot create an index of compressed formats.'

        # load the index
        self.indexfile = indexfile or to_indexfile(self.name)
        self.ixdb = dataset.connect(dbtype + self.indexfile)
        self.ixtable = self.ixdb[self.INDEX_TABLE]

        # user defined function to add extra metadata to table
        self._extraattrs = attrs

        # maybe auto create index
        if autoindex and (
                # no entries in the table
                not len(self.ixtable) or
                # the file datetime is different than the time stored in the index
                int(os.path.getmtime(self.name)) !=
                    self.ixdb.query(GET_APP_ID)):
            self.build_index()


    def _getmember(self, name, tarinfo=None, normalize=False):
        """Find an archive member by name from bottom to top.
           If tarinfo is given, it is used as the starting point.
        """
        # already a member object
        if isinstance(name, trf.TarInfo):
            return name

        # load position from index
        query = dict(name=name)
        if tarinfo: # the limit is only there to be consistent
            query['offset_header'] = {'<': tarinfo.offset_header}

        res = self.ixtable.find_one(**query)
        if res is None: # nothing found
            return

        # save the original offset to revert to
        prev_offset = self.offset

        # get member
        self.fileobj.seek(res['offset_header'])
        member = self.tarinfo.fromtarfile(self)
        self.offset = prev_offset # revert
        return member


    def build_index(self, nram=500, index_cols=('name',)):
        '''Build a sqlite index mapping the files in the tar'''
        # preserve the original
        prev_members, self.members = self.members, []

        # loop over each file and add it to the index
        size, unit = humansize(os.path.getsize(self.name))
        with tqdm(total=size, unit=unit) as pbar:
            for i, member in enumerate(self):
                self.add_member_to_index(member)
                pbar.update(humansize(member.offset_data, unit)[0] - pbar.n)
                if not i % nram: # free ram...
                    self.members = []

        # create the table index for faster name lookup and store the modified time
        self.ixtable.create_index(index_cols)
        self.ixdb.query(SET_APP_ID.format(os.path.getmtime(self.name)))

        # reset members
        self.members = prev_members

    def remove_index(self):
        self.ixdb.executable.close()
        os.remove(self.indexfile)

    '''

    Boring Overrides

    '''

    def addfile(self, *a, **kw):
        # overriding to add new files to the index
        # ASSUMPTION: self.members does not lose anything during super.addfile
        n = len(self.members)
        super().addfile(*a, **kw)
        for m in self.members[n:]:
            self.add_member_to_index(m)

    def getnames(self):
        return self.ixtable.distinct('name')

    def close(self, *a, **kw):
        super().close(*a, **kw)
        self.ixdb.executable.close()

    '''

    Helpers

    '''

    def querymember(self, *a, **kw):
        '''Select a tar member based on a query.

        Examples and docs for building queries:
            https://dataset.readthedocs.io/en/latest/quickstart.html#reading-data-from-tables
        '''
        return self.ixtable.find_one(*a, **kw)

    def queryfile(self, *a, **kw):
        '''Select a tar file based on a query.

        Examples and docs for building queries:
            https://dataset.readthedocs.io/en/latest/quickstart.html#reading-data-from-tables
        '''
        return self.extractfile(self.querymember(*a, **kw))

    def clear_members(self):
        self.offset = 0
        self.members = []
        self._loaded = False
        self.firstmember = None
        self.fileobj.seek(0)



    def add_member_to_index(self, member):
        '''Add a tarinfo object to the index'''
        d = {
            'name': member.name,
            'offset_header': member.offset,
            'offset_data': member.offset_data,
            'size': member.size,
        }
        # add user defined attributes
        self._extraattrs and d.update(self._extraattrs(member))
        return self.ixtable.insert(d)





# sqlite queries
SET_APP_ID = "PRAGMA application_id = {:.0f}"
GET_APP_ID = 'PRAGMA application_id'


'''

Utils

'''
def to_indexfile(tarfile):
    '''Create an index file automatically from the tarfile'''
    return tarfile + '.index'



_units = [u + 'b' for u in ('',) + tuple('kmgtpezy')]
def humansize(x, unit=None):
    '''Get byte size in human readable form (e.g. mb, gb)'''
    for u in _units:
        if (unit == u if unit else x < 1024):
            break
        x /= 1024
    return x, u



def main():
    '''
    Usage:
        create index file:
            taridx tarfile.tar build_index
        lookup file using indexfile:
            taridx tarfile.tar get some/file_you.want
    '''

    # like magic
    import fire
    fire.Fire(IndexedTarFile.open)

if __name__ == "__main__":
    main()
