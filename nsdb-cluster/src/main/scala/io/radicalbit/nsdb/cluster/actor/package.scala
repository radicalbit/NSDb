package io.radicalbit.nsdb.cluster

package object actor {

  /**
    * Namespace identifier.
    * @param db enclosing db.
    * @param namespace namespace name.
    */
  case class NamespaceKey(db: String, namespace: String)

}
