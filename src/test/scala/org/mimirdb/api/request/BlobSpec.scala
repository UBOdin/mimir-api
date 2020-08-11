package org.mimirdb.api.request

import java.io.ByteArrayInputStream
import play.api.libs.json._
import org.specs2.mutable.Specification
import org.specs2.specification.BeforeAll

import org.mimirdb.api.SharedSparkTestInstance
import org.mimirdb.api.MimirAPI


class BlobSpec 
  extends Specification
  with SharedSparkTestInstance
  with BeforeAll
{

  def beforeAll = SharedSparkTestInstance.initAPI

  "BlobStore" >> {

    val name = "FizzBuzz_1234243562346"
    val t = "FizzyPop"
    val lorem = "Lorem ipsum dolor sit amet, consectetur adipiscing elit. In nec elit vitae massa faucibus gravida vitae et ex. Aliquam molestie porta libero, vitae consequat dui rhoncus consequat. In imperdiet vehicula mi at ornare. Phasellus semper fringilla orci, tincidunt placerat arcu varius vitae. Donec non tincidunt nibh. Fusce sed tellus quis velit pretium iaculis. Cras ante lorem, vulputate non metus quis, vestibulum posuere erat. Fusce ac semper tortor, in ornare dui. Phasellus varius sagittis urna, sed elementum leo. Vivamus quam turpis, porttitor sit amet laoreet sit amet, vestibulum vel mauris. Aenean eget felis in purus maximus ullamcorper faucibus et quam. Vivamus urna dui, elementum ac gravida id, malesuada id erat. Suspendisse malesuada tincidunt hendrerit. Suspendisse commodo quam id ultrices bibendum. Nunc et est vitae arcu posuere suscipit vel eget mi.".getBytes()

    val create = CreateBlobRequest(
      new ByteArrayInputStream(lorem),
      Some(t),
      Some(name)
    ).handle

    val get = GetBlobRequest(
      id = name
    ).handle

    get.blobType must beEqualTo(t)
    get.data must beEqualTo(lorem)

  }

}