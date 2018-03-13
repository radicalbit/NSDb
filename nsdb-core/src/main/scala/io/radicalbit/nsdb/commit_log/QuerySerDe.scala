package io.radicalbit.nsdb.commit_log

import io.radicalbit.nsdb.common.JSerializable

object QuerySerDe {

    type QueryClass = String
    type QueryTermClass = String
    type QueryTermName = String
    type QueryTerm = (QueryTermClass, QueryTermName)
    type QueryTermValue = JSerializable
    type Query = (QueryClass, QueryTerm, Seq[QueryTermValue])

    def serialize(query: Query): Array[Byte] = {

    }

}
