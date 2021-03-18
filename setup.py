from setuptools import find_packages, setup

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name='neubase',
    url='https://github.com/ajb1970/neubase',
    author='Andrew Baisley',
    author_email='andrew.baisley@gmail.com',
    package_dir={"": "src"},
    packages=find_packages(where="src"),
    version='0.1',
    license='MIT',
    description='A wrapper for SQLite databases and Pandas DataFrames intended to store DfE data tables.',
)