from setuptools import find_packages, setup

setup(
    name="jaswanth_momentum_tracker",
    packages=find_packages(exclude=["jaswanth_momentum_tracker_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud"
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
