import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="MegafonAPI-robotkarl", # Replace with your own username
    version="0.0.1",
    author="Ilya Strukov",
    author_email="ilya@strukov.net",
    description="Python API implementation to work with Megafon business services",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/robotkarl/MegafonAPI",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
)