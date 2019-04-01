import setuptools

with open("README.md", "r") as fh:
	long_description=fh.read()

setuptools.setup(
		name='db_adapter',
		version='0.0.1',
		scripts=['db_adapter'],
		author="Shadhini Jayatilake",
		author_email="shadhini.jayatilake@gmail.com",
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
		)
