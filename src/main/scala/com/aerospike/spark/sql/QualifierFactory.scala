package com.aerospike.spark.sql

import java.util.Calendar

import com.aerospike.client.Value
import com.aerospike.helper.query.{LatestUpdateQualifier, Qualifier}

object QualifierFactory {
  def create(attribute: String, operation: Qualifier.FilterOperation, value: Any) : Qualifier = {
    if (attribute == "__lut") {
      val time = value match {
        case calendar: Calendar =>
          calendar.getTimeInMillis * 1000000 // we need time in nanoseconds
        case _ =>
          value.asInstanceOf[Long]
      }
      return new LatestUpdateQualifier(operation, Value.get(time))
    } else {
      return new Qualifier(attribute, operation, Value.get(value))
    }
  }

}
