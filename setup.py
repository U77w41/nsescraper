"""setup"""
import pathlib
from setuptools import setup, find_packages

HERE = pathlib.Path(__file__).parent.resolve()

# Get the long description from the README file
long_description = (HERE / 'README.md').read_text(encoding='utf-8')
__version__ = '0.0.3'
__maintainer__ = 'Ujjwal Chowdhury'


# Setting up
setup(
    name='nsescraper',
    version=__version__,
    description='A Scraper for https://www.nseindia.com',
    author=__maintainer__,
    author_email='<u77w41@gmail.com>',
    url='https://github.com/U77w41/',
    license='MIT',
    long_description=long_description,
    long_description_content_type='text/markdown',
    packages=find_packages(),
    include_package_data = True,
    data_files=[
        ('nsescraper',['nsescraper/nifty_indices.pickle']),
    ],
    install_requires=['pandas','pytz'],
    tests_require=['pytest'],
    keywords= ['python','NSE','NIFTY','scraping']
)

#################################################################################################################
# python3 setup.py sdist bdist_wheel
# twine upload dist/*