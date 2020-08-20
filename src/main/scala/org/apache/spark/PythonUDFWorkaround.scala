package org.apache.spark


import java.util.{ArrayList => JArrayList, List => JList, Map => JMap}
import org.apache.spark.sql.types._
import org.apache.spark.api.python.PythonFunction
import org.apache.spark.sql.catalyst.expressions.{ PythonUDF, Expression, ExprId, NamedExpression }
import org.apache.spark.api.python.PythonAccumulatorV2

object PythonUDFWorkaround
{
  def apply(
    command: Array[Byte],
    envVars: JMap[String, String],
    pythonIncludes: JList[String],
    pythonExec: String,
    pythonVer: String
  )(
    name: String,
    dataType: DataType,
    children: Seq[Expression],
    evalType: Int,
    udfDeterministic: Boolean,
    resultId: ExprId = NamedExpression.newExprId
  ): PythonUDF =
    PythonUDF(
      name = name,
      func = PythonFunction(
        command = command,
        envVars = envVars,
        pythonIncludes = pythonIncludes,
        pythonExec = pythonExec,
        pythonVer = pythonVer,
        broadcastVars = new JArrayList(),
        accumulator = null
      ),
      dataType = dataType,
      children = children,
      evalType = evalType,
      udfDeterministic = udfDeterministic,
      resultId = resultId
    )
}