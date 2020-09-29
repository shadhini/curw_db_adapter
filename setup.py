import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
        name='db_adapter',
        version='1.1.0',
        author="Shadhini Jayatilake",
        author_email="shadhini.jayatilake@gmail.com",
        license='MIT',
        description="MySQL Database utility package for Centre for Urban Water",
        long_description=long_description,
        long_description_content_type="text/markdown",
        url="https://github.com/shadhini/curw_db_adapter",
        packages=setuptools.find_packages(),
        classifiers=[
                "Programming Language :: Python :: 3",
                "License :: OSI Approved :: MIT License",
                "Operating System :: OS Independent",
                ],
        install_requires=['PyMySQL==0.9.3',
                          'pandas==0.24.2',
                          'numpy==1.16.2',
                          'PyYAML==5.1',
                          'aenum==2.1.2',
                          'DBUtils==1.3'],
        zip_safe=False,
        include_package_data=True  # to add non-code files specified in "MANIFEST.in" file
        )
