"""setup"""
import pathlib
from setuptools import setup, find_packages

HERE = pathlib.Path(__file__).parent.resolve()

# Get the long description from the README file
long_description = (HERE / 'README.md').read_text(encoding='utf-8')
__version__ = '0.0.7'
__maintainer__ = 'Ujjwal Chowdhury'


# Setting up
setup(
    name='nsescraper',
    version=__version__,
    description='A scraper for https://www.nseindia.com',
    author=__maintainer__,
    author_email='<u77w41@gmail.com>',
    url='https://github.com/U77w41/',
    license='MIT',
    long_description=long_description,
    long_description_content_type='text/markdown',
    packages=find_packages(),
    classifiers=[
    'Programming Language :: Python :: 3',
    'License :: OSI Approved :: MIT License',
    'Operating System :: OS Independent'],
    include_package_data = True,
    python_requires='>=3.8',
    data_files=[
        ('nsescraper',['nsescraper/nifty_indices.pickle','nsescraper/option_indices.pickle','nsescraper/nsescraper.svg']),
    ],
    install_requires=['pandas','pytz','urllib3'],
    tests_require=['pytest'],
    keywords= ['python','NSE','NIFTY','scraping']
)

#################################################################################################################
# python3 setup.py sdist bdist_wheel
# twine upload dist/*
