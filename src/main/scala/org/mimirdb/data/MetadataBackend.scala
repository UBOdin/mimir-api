package org.mimirdb.data

import java.sql._

abstract class MetadataBackend {

  def close(): Unit

  def registerMap(
    category: String, 
    migrations: Seq[MapMigration]
  ): MetadataMap
  def registerManyMany(category: String): MetadataManyMany

  def keysForMap(category: String): Seq[String]
  def allForMap(category: String): Seq[Metadata.MapResource]
  def getFromMap(category: String, resource: String): Option[Metadata.MapResource]
  def existsInMap(category: String, resource: String): Boolean = getFromMap(category, resource) != None
  def putToMap(category: String, resource: Metadata.MapResource): Unit
  def rmFromMap(category: String, resource: String): Unit
  def updateMap(category: String, resource: String, body:Map[String, Any]): Unit

  def addToManyMany(category: String, lhs:String, rhs: String): Unit
  def getManyManyByLHS(category: String, lhs:String): Seq[String]
  def getManyManyByRHS(category: String, rhs:String): Seq[String]
  def rmFromManyMany(category: String, lhs:String, rhs: String): Unit
  def rmByLHSFromManyMany(category: String, lhs: String): Unit
  def rmByRHSFromManyMany(category: String, rhs: String): Unit
}

class MetadataMap(backend: MetadataBackend, category: String)
{
  def keys: Seq[String]                                  = backend.keysForMap(category)
  def all: Seq[Metadata.MapResource]                     = backend.allForMap(category)
  def get(resource:String): Option[Metadata.MapResource] = backend.getFromMap(category, resource)
  def exists(resource: String): Boolean                  = backend.existsInMap(category, resource)
  def put(resource: Metadata.MapResource)                = backend.putToMap(category, resource)
  def put(id: String, body: Seq[Any])                    = backend.putToMap(category, (id, body))
  def update(id: String, body: Map[String, Any])         = backend.updateMap(category, id, body)
  def rm(resource: String)                               = backend.rmFromMap(category, resource)
}

class MetadataManyMany(backend: MetadataBackend, category: String)
{
  def add(lhs: String, rhs: String)      = backend.addToManyMany(category, lhs, rhs)
  def getByLHS(lhs: String): Seq[String] = backend.getManyManyByLHS(category, lhs)
  def getByRHS(rhs: String): Seq[String] = backend.getManyManyByRHS(category, rhs)
  def rm(lhs: String, rhs: String)       = backend.rmFromManyMany(category, lhs, rhs)
  def rmByLHS(lhs: String)               = backend.rmByLHSFromManyMany(category, lhs)
  def rmByRHS(rhs: String)               = backend.rmByRHSFromManyMany(category, rhs)
}