from setuptools import setup, find_packages

setup(
    name='sparkbuilder',
    version='0.1',
    description='A package for building Spark jobs',
    author='Databricks / Campbells',
    author_email='',
    packages=find_packages(),
    classifiers=[
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
    ],
    python_requires='>=3.6',
)