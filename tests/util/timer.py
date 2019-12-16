import time
import numpy as np
from contextlib import contextmanager



class Timer:
    '''Measure code execution time.

    Example:
    >>> timer = Timer()
    >>>
    >>> timer.tick('open')
    >>> with open(file) as f:
    ...     timer.tock('open')
    ...
    ...     timer.tick('read')
    ...     d = f.read()
    ...     timer.tocktick('read')
    ...     d = f.read()
    ...     timer.tock('read')
    ...
    >>>     with timer.ticktock('read'):
    ...         d = f.read()
    '''
    def __init__(self, verbose=False):
        self.verbose = verbose
        self.starts = {}
        self.start = time.time()
        self.elapsed = {}

    def tick(self, *names, verbose=False):
        '''Start the timer'''
        for name in names:
            self.starts[name] = time.time()
            if name not in self.elapsed:
                self.elapsed[name] = []

            if verbose or self.verbose:
                print('tick:', name)
        return self

    def tock(self, *names, verbose=False):
        '''Record the execution time'''
        t = time.time()
        for name in names:
            dt = t - (self.starts.get(name) or self.start)
            self.elapsed[name].append(dt)

            if verbose or self.verbose:
                print('tock: {} ({:.2f}s)'.format(name, dt))
        return self

    @contextmanager
    def ticktock(self, name):
        '''Measure the execution time within a ``with`` block.'''
        self.tick(name)
        yield self
        self.tock(name)

    def tocktick(self, *names):
        '''Record time since last measurement and reset the time marker.
        '''
        self.tock(*names)
        self.tick(*names)
        return self

    def results(self):
        '''Get time aggregations for all measured time intervals.'''
        results = {
            name: {
                'min': np.min(times),
                'max': np.max(times),
                'mean': np.mean(times),
                'std': np.std(times),
                'median': np.median(times),
                '10%': np.percentile(times, 10),
                '90%': np.percentile(times, 90),
                'count': len(times),
            }
            for name, times in self.elapsed.items()
        }
        results['total_time'] = time.time() - self.start
        return results
