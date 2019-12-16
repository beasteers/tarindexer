import setuptools

setuptools.setup(name='tarindexer',
                 version='0.0.1',
                 description='Tar indexing for fast random file access',
                 # long_description=open('README.md').read().strip(),
                 author='Bea Steers',
                 author_email='bea.steers@gmail.com',
                 # url='http://path-to-my-packagename',
                 packages=setuptools.find_packages(),
                 install_requires=['tqdm', 'dataset'],
                 license='MIT License',
                 entry_points={
                        'console_scripts': ['taridx = taridx.taridx:main'],
                 },
                 keywords='tar index random access ')
