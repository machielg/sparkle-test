from pybuilder.core import use_plugin, init, Project, Author

use_plugin("python.core")
use_plugin("python.unittest")
use_plugin("python.install_dependencies")
use_plugin("python.flake8")
# use_plugin("python.coverage")
use_plugin("python.distutils")

name = "sparkle-test"
summary = "A small and simple base class for fast and clean PySpark unit tests"
description = """
Unit testing in Spark is made easier with sparkle-test, the settings are tuned for performance and your unit tests
don't leave any files in your workspace. There is one convenience method for asserting dataframe equality.
"""
default_task = "publish"
version = "1.0.0"

url = "https://github.com/machielg/sparkle-test/"
licence = "GPLv3+"


authors = [Author("Machiel Keizer Groeneveld", "machielg@gmail.com")]
@init
def set_properties(project: Project):
    project.depends_on('pandas')
    project.build_depends_on('pyspark')

    project.set_property("distutils_classifiers", [
        "License :: OSI Approved :: GNU General Public License v3 or later (GPLv3+)",
        "Environment :: Console",
        'Programming Language :: Python :: 3.6',
        'Topic :: Software Development :: Testing'
    ])
