"""afs_mission_goal."""

from pathlib import Path
from setuptools import find_packages
from setuptools import setup


def read_lines(path):
    """Read lines of `path`."""
    with open(path) as f:
        return f.read().splitlines()


BASE_DIR = Path(__file__).parent


setup(
    name="afs_mission_goal",
    long_description=open(BASE_DIR / "README.md").read(),
    install_requires=read_lines(BASE_DIR / "requirements.txt"),
    extras_require={"dev": read_lines(BASE_DIR / "requirements_dev.txt")},
    packages=find_packages(exclude=["docs"]),
    version="0.1.0",
    description="A project to investigate who the children are who are not reaching age-related expectations. We will look at this across England, Scotland and Wales, and will investigate the characteristics of these families, better understanding the circumstances that these children are growing up in. We can use this to redefine our mission goal, and ensure it accurately reflects the families and children we want to support across all the nations.",
    author="Nesta",
    license="proprietary",
)
