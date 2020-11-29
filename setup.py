from setuptools import setup, find_packages

with open("README.md", "r") as fh:
    long_description = fh.read()

setup(
    name="MegafonAPI",
    version="0.0.41",
    author="Ilya Strukov",
    author_email="ilya@strukov.net",
    description="Python API implementation to work with Megafon business services",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/robotkarl/MegafonAPI",
    install_requires=[
        "certifi==2020.6.20",
        "chardet==3.0.4",
        "cssselect==1.1.0; python_version >= '2.7' and python_version not in '3.0, 3.1, 3.2, 3.3'",
        "idna==2.10; python_version >= '2.7' and python_version not in '3.0, 3.1, 3.2, 3.3'",
        "lxml==4.5.2; python_version >= '2.7' and python_version not in '3.0, 3.1, 3.2, 3.3, 3.4'",
        "pyquery==1.4.1",
        "pytz==2020.1",
        "requests==2.24.0",
        "urllib3==1.25.10",
    ],
    packages=find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.6",
)
