from setuptools import find_packages, setup

setup(
    name="pipeline_olist",
    packages=find_packages(exclude=["pipeline_olist_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud"
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
