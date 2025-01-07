from setuptools import setup, find_packages

setup(
    name="dynawrap",
    version="1.0.0",
    description="Lightweight wrapper to handle access pattern management to AWS DynamoDB tables",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    author="Ed Grundy",
    author_email="ed@bayis.co.uk",
    url="https://github.com/bayinfosys/aws-dynamodb-wrapper",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.7",
    install_requires=[
        "boto3>=1.20.0",
    ],
    extras_require={
        "dev": ["pytest", "black", "flake8"],
    },
)
