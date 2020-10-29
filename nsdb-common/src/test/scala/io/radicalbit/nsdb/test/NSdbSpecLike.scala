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

package io.radicalbit.nsdb.test

import org.scalatest.flatspec.{AnyFlatSpec, AnyFlatSpecLike}
import org.scalatest.matchers.should
import org.scalatest.wordspec.{AnyWordSpec, AnyWordSpecLike}

/**
  * Mix-in scalatest [[AnyWordSpecLike]] with should matchers
  */
trait NSDbSpecLike extends AnyWordSpecLike with should.Matchers

/**
  * Mix-in scalatest [[AnyFlatSpec]] with should matchers
  */
trait NSDbFlatSpec extends AnyFlatSpec with should.Matchers

/**
  * Mix-in scalatest [[AnyFlatSpecLike]] with should matchers
  */
trait NSDbFlatSpecLike extends AnyFlatSpecLike with should.Matchers

/**
  * Mix-in scalatest [[AnyWordSpec]] with should matchers
  */
trait NSDbSpec extends AnyWordSpec with should.Matchers
