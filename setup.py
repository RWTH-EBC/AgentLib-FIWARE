import setuptools


INSTALL_REQUIRES = [
    "filip>=0.3.0",
    'agentlib>=0.8.2',
]

setuptools.setup(
    name="cd .",
    version="0.1.0",
    author="Associates of the AGENT project",
    author_email="AGENT.Projekt@eonerc.rwth-aachen.de",
    description="Plugin for the agentlib. Includes tools to communicate with FIWARE",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Operating System :: OS Independent",
    ],
    install_requires=INSTALL_REQUIRES,
)
