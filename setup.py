from setuptools import setup, find_packages

setup(
    name="covid_school_data",
    version="1.0",
    packages=find_packages(),
    package_data={
        "school_data_mapper": ["*.conf"]
    },
    include_package_data=True,
    install_requires=[
        'pandas',
        'numpy',
        'recordclass',
        'absl-py',
    ],
)
