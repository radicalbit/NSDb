/*
 * Copyright 2018-2020 Radicalbit S.r.l.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.radicalbit.nsdb.sql.parser

import com.fasterxml.jackson.annotation.{JsonSubTypes, JsonTypeInfo}
import io.radicalbit.nsdb.common.protocol.NSDbSerializable
import io.radicalbit.nsdb.common.statement.{CommandStatement, SQLStatement}

object StatementParserResult {

  @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
  @JsonSubTypes(
    Array(
      new JsonSubTypes.Type(value = classOf[SqlStatementParserSuccess], name = "SqlStatementParserSuccess"),
      new JsonSubTypes.Type(value = classOf[SqlStatementParserFailure], name = "SqlStatementParserFailure")
    ))
  trait SqlStatementParserResult

  case class SqlStatementParserSuccess(queryString: String, statement: SQLStatement)
      extends SqlStatementParserResult
      with NSDbSerializable
  case class SqlStatementParserFailure(queryString: String, error: String)
      extends SqlStatementParserResult
      with NSDbSerializable

  @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
  @JsonSubTypes(
    Array(
      new JsonSubTypes.Type(value = classOf[CommandStatementParserSuccess], name = "CommandStatementParserSuccess"),
      new JsonSubTypes.Type(value = classOf[CommandStatementParserFailure], name = "CommandStatementParserFailure")
    ))
  trait CommandStatementParserResult

  case class CommandStatementParserSuccess(inputString: String, statement: CommandStatement)
      extends CommandStatementParserResult
      with NSDbSerializable
  case class CommandStatementParserFailure(inputString: String, error: String)
      extends CommandStatementParserResult
      with NSDbSerializable

}
