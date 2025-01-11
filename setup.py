from setuptools import setup, find_packages
import os

# Read the contents of README file
with open("README.md", encoding="utf-8") as f:
    long_description = f.read()

# Read the contents of requirements.txt
with open("requirements.txt", encoding="utf-8") as f:
    requirements = [line.strip() for line in f if line.strip() and not line.startswith("#")]

setup(
    name="tenori",
    version="1.1.1",
    author="Tanaka Chinengundu",
    author_email="tanakah30@gmail.com",
    description="A Flask package for easily adding multi-tenancy with dedicated databases",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/TaqsBlaze/tenori",
    packages=find_packages(exclude=["tests*", "examples*"]),
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Framework :: Flask",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Topic :: Database",
    ],
    python_requires=">=3.10",
    install_requires=[
        "Flask>=2.0.0",
        "Flask-SQLAlchemy>=2.5.0",
        "SQLAlchemy>=1.4.0",
        "PyMySQL>=1.0.0",
    ],
    extras_require={
        "dev": [
            "pytest>=6.0.0",
            "pytest-cov>=2.0.0",
            "black>=21.0.0",
            "flake8>=3.9.0",
            "mypy>=0.900",
        ],
    },
    project_urls={
        "Bug Reports": "https://github.com/TaqsBlaze/tenori/issues",
        "Source": "https://github.com/TaqsBlaze/tenori",
        "Documentation": "https://github.com/TaqsBlaze/tenori#readme",
    },
    keywords=["flask", "multitenant", "database", "sqlalchemy", "tenant", "saas"],
)
