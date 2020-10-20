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


def invoke_vizier_udf(*args):
  fn = args[0]
  args = args[1:]
  class VizierUDFWrapper:
    def __init__(self):
      self.fn = None
    def export_module_decorator(self, fn):
      self.fn = fn
      return fn
  vizierdb = VizierUDFWrapper()
  exec(fn)
  return vizierdb.fn(*args)

test_case = """
@vizierdb.export_module_decorator
def apply_foo(a):
    return a + 1
"""

# test it
assert(invoke_vizier_udf(test_case, 1) == 2)


export = cloudpickle.dumps(invoke_vizier_udf)

# print(sys.argv)
with open(export_file, "wb") as out_file:
  out_file.write(export)