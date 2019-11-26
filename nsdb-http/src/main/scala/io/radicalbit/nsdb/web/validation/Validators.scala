/*
 * Copyright 2018 Radicalbit S.r.l.
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

package io.radicalbit.nsdb.web.validation

import io.radicalbit.nsdb.sql.parser.RegexNSDb
import io.radicalbit.nsdb.web.routes.InsertBody

object Validators extends RegexNSDb {

  implicit val insertBodyValidator: Validator[InsertBody] = new Validator[InsertBody] {

    private def metricRule(metricName: String): Boolean = !metricName.matches(metric.regex)

    def apply(insertBody: InsertBody): Seq[FieldErrorInfo] = {
      val metricErrorOpt =
        validationStage(metricRule(insertBody.metric), "metric", "Field metric must contain only alphanumeric chars")
      (metricErrorOpt :: Nil).flatten
    }
  }
}
