from setuptools import setup

setup(
    # Needed to silence warnings (and to be a worthwhile package)
    name='neubase',
    url='https://github.com/ajb1970/neubase',
    author='Andrew Baisley',
    author_email='andrew.baisley@gmail.com',
    # Needed for dependencies
    install_requires=['pandas'],
    # *strongly* suggested for sharing
    packages=['neubase'],
    package_dir={"": "src"},
    packages=setuptools.find_packages(where="src"),
    version='0.1',
    # The license can be anything you like
    license='MIT',
    description='A wrapper for SQLite databases and Pandas DataFrames intended to store DfE data tables.',
    # We will also need a readme eventually (there will be a warning)
    # long_description=open('README.txt').read(),
)