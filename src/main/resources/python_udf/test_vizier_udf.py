import sys
from os import path
from pyspark import cloudpickle

if len(sys.argv) == 1:
  export_file = path.join(path.dirname(sys.argv[0]), "call_vizier_udf.pickle")
elif len(sys.argv) == 2:
  export_file = sys.argv[1]
else:
  print("usage: python3 export_vizier_udf.py [target_file]")
  exit(-1)

test_case = """
@vizierdb.export_module_decorator
def apply_foo(a):
    return a + 1
"""

with open(export_file, "rb") as in_file:
  invoke_vizier_udf = cloudpickle.loads(in_file.read())

assert(invoke_vizier_udf(test_case, 1) == 2)

print("Success!")