import scala.language.implicitConversions

trait FilterCond {def eval(r: Row): Option[Boolean]}

case class Field(colName: String, predicate: String => Boolean) extends FilterCond {
  override def eval(r: Row): Option[Boolean] = Some(predicate(r(colName)))
}

case class Compound(op: (Boolean, Boolean) => Boolean, conditions: List[FilterCond]) extends FilterCond {
  override def eval(r: Row): Option[Boolean] = {
    def compare(conditions: List[FilterCond]): Boolean = {
      conditions match
        //  Last element
        case x :: Nil =>
          x.eval(r) match
            case Some(statement) => statement
            case None => false
        //  Any other element
        case x :: xs =>
          x.eval(r) match
            case Some(statement) => op(statement, compare(xs))
            case None => op(false, compare(xs))
        //  Error
        case Nil => false
    }

    Some(compare(conditions))
  }
}

case class Not(f: FilterCond) extends FilterCond {
  override def eval(r: Row): Option[Boolean] =
    f.eval(r) match
      case Some(bool) => Some(!bool)
      case None => None
}

def And(f1: FilterCond, f2: FilterCond): FilterCond =
  Compound(_ && _, List(f1, f2))
def Or(f1: FilterCond, f2: FilterCond): FilterCond =
  Compound(_ || _, List(f1, f2))
def Equal(f1: FilterCond, f2: FilterCond): FilterCond =
  Compound(_ == _, List(f1, f2))

case class Any(fs: List[FilterCond]) extends FilterCond {
  override def eval(r: Row): Option[Boolean] =
    Compound(_ || _, fs).eval(r)
}

case class All(fs: List[FilterCond]) extends FilterCond {
  override def eval(r: Row): Option[Boolean] =
    Compound(_ && _, fs).eval(r)
}

implicit def tuple2Field(t: (String, String => Boolean)): Field = Field(t._1, t._2)

extension (f: FilterCond) {
  def ===(other: FilterCond) = Equal(f, other)
  def &&(other: FilterCond) = And(f, other)
  def ||(other: FilterCond) = Or(f, other)
  def !! = Not(f)
}