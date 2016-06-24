package com.criteo.scalaschemas.scalding.job

import com.twitter.scalding.{Args, Job}

/**
  * Functions for transforming Stringly typed args to a target type.
  *
  * @tparam T The target type
  */
trait TypedArgsParser[T] {

  def args2TypedArgs(args: Array[String]): T = args2TypedArgs(Args(args))

  def args2TypedArgs(args: Args): T

}

trait TypedJobArgs[T] extends Job with TypedArgsParser[T] {

  // in theory this could be a lazy val, but that breaks the serialization of jobs
  def typedArgs: T = args2TypedArgs(args)

}

case class RootArgs(root: String)

trait RootArgsParser extends TypedArgsParser[RootArgs] {
  override def args2TypedArgs(args: Args): RootArgs = RootArgs(args.required("root"))
}

/**
  * Provides typedArgs of type [[RootArgs]] to a scalding [[Job]].
  *
  * eg, class MyScaldingJob(args: Args) extends Job(args) with RootJobArgs
  *
  */
trait RootJobArgs extends TypedJobArgs[RootArgs] with RootArgsParser