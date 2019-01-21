from pybuilder.core import use_plugin, init

use_plugin("python.core")
use_plugin("python.unittest")
use_plugin("python.install_dependencies")
use_plugin("python.flake8")
# use_plugin("python.coverage")
use_plugin("python.distutils")

name = "pyspark-testing"
default_task = "publish"
version = "0.0.1.dev"


@init
def set_properties(project):
    pass
