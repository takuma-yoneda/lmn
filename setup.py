import pathlib

from setuptools import find_packages, setup

BASE_DIR = pathlib.Path(__file__).resolve().parent
exec((BASE_DIR / "lmd/_version.py").read_text())


setup(
    name="lmd",
    version=__version__,  # type: ignore[name-defined]  # NOQA: F821
    packages=find_packages(),
    description=(
        "Remote code execution for ML enthusiasts."
    ),
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    author="Takuma Yoneda",
    author_email="takuma.ynd@gmail.com",
    url="https://github.com/takuma-yoneda/lmd",
    license="MIT License",
    classifiers=[
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python",
        "License :: OSI Approved :: MIT License",
        "Operating System :: POSIX",
        "Operating System :: MacOS",
        "Operating System :: Unix",
    ],
    install_requires=["fabric", "docker", "randomname"],
    # extras_require={
    #     "lint": [
    #         "black>=19.10b0,<=20.8",
    #         "flake8-bugbear",  # flake8 doesn't have a dependency for bugbear plugin
    #         "flake8>=3.7,<5",
    #         "isort>=4.3,<5.2.0",
    #         "mypy>=0.770,<0.800",
    #     ],
    # },
    # package_data={"pysen": ["py.typed"]},
    package_dir={'lmd': 'lmd'},
    entry_points={"console_scripts": ["lmd=lmd.cli.main:run"]},
)
