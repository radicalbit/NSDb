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

package io.radicalbit.nsdb.util

object ErrorManagementUtils {

  /**
    * Filters a generic collection of responses, returns the successes and apply a behaviour to the failures.
    * @param results the generic collection of responses.
    * @param errorBehaviour a function to be applied to the list of errors found.
    * @tparam S the success type.
    * @tparam F the failure type.
    * @return the list of succeeded responses.
    */
  def manageErrors[S, F](results: Traversable[Any])(errorBehaviour: Seq[F] => Unit)(implicit s: Manifest[S],
                                                                                    f: Manifest[F]): List[S] = {
    val (successes, errors) = partitionResponses(results)(s, f)
    if (errors.nonEmpty) {
      errorBehaviour(errors)
    }
    successes
  }

  /**
    * Filters a collection of [[Either]] typed responses , returns the successes and apply a behaviour to the failures.
    * @param results the generic collection of responses.
    * @param errorBehaviour a function to be applied to the list of errors found.
    * @tparam S the success type.
    * @tparam F the failure type.
    * @return the list of succeeded responses.
    */
  def manageErrors[S, F](results: Traversable[Either[F, S]])(errorBehaviour: Seq[F] => Unit)(
      implicit d: DummyImplicit): List[S] = {
    val (successes, errors) = partitionResponses(results)

    if (errors.nonEmpty) {
      errorBehaviour(errors)
    }
    successes
  }

  /**
    * Partitions a generic collection of responses, separating errors and successes.
    * @param responses the generic collection of responses.
    * @tparam S the success type.
    * @tparam F the failure type.
    * @return a tuple containing the succeeded and the failures .
    */
  def partitionResponses[S, F](responses: Traversable[Any])(implicit s: Manifest[S],
                                                            f: Manifest[F]): (List[S], List[F]) =
    responses.foldRight((List.empty[S], List.empty[F])) {
      case (f, (successAcc, errorAcc)) =>
        if (manifest[S].runtimeClass.isInstance(f))
          (f.asInstanceOf[S] :: successAcc, errorAcc)
        else
          (successAcc, f.asInstanceOf[F] :: errorAcc)
    }

  /**
    * Partitions a collection of [[Either]] typed responses, separating errors and successes.
    * @param responses the generic collection of responses.
    * @tparam S the success type.
    * @tparam F the failure type.
    * @return a tuple containing the succeeded and the failures .
    */
  def partitionResponses[S, F](responses: Traversable[Either[F, S]])(implicit d: DummyImplicit): (List[S], List[F]) =
    responses.foldRight((List.empty[S], List.empty[F])) {
      case (f, (successAcc, errorAcc)) =>
        f match {
          case Right(success) => (success :: successAcc, errorAcc)
          case Left(error)    => (successAcc, error :: errorAcc)
        }
    }

}
