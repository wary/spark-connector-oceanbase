/*
 * Copyright 2024 OceanBase.
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

package org.apache.spark.sql.jdbc

object BuiltInDialectCompat {

  def load(className: String): JdbcDialect = {
    val dialectClass = Class.forName(className)
    moduleInstance(dialectClass).getOrElse {
      val companion = Class.forName(className + "$").getField("MODULE$").get(null)
      companion.getClass.getMethod("apply").invoke(companion).asInstanceOf[JdbcDialect]
    }
  }

  private def moduleInstance(dialectClass: Class[_]): Option[JdbcDialect] = {
    try {
      Some(dialectClass.getField("MODULE$").get(null).asInstanceOf[JdbcDialect])
    } catch {
      case _: NoSuchFieldException => None
    }
  }
}
