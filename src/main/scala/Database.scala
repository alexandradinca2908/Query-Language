import scala.annotation.tailrec

case class Database(tables: List[Table]) {
  override def toString: String = {
    val result = tables.foldLeft("")((str, t) => str + t + '\n')
    result.substring(0, result.length - 1)
  }

  def exists(allTables: List[Table], tableName: String): Boolean =
    allTables match
      case head :: tail =>
        if (head.name == tableName) true
        else exists(tail, tableName)
      case Nil => false

  def create(tableName: String): Database =
    if (exists(tables, tableName)) Database(tables)
    else {
      val newTableList = tables ++ List(new Table(tableName, List.empty[Row]))
      Database(newTableList)
    }

  def drop(tableName: String): Database = {
    val newTableList = tables.filter(_.name != tableName)
    Database(newTableList)
  }

  def selectTables(tableNames: List[String]): Option[Database] = {
    @tailrec
    def searchTable(tableName: String, allTables: List[Table]): Option[Table] = {
      allTables match
        case head :: tail =>
          if (head.name == tableName) Some(head)
          else searchTable(tableName, tail)
        case Nil => None
    }
    val tableListOption: List[Option[Table]] =
      tableNames.foldRight(List.empty[Option[Table]])((x, xs) => searchTable(x, tables) :: xs)

    if (tableListOption.contains(None)) None
    else {
      val newTableList: List[Table] = tableListOption.flatten
      Some(Database(newTableList))
    }
  }

  def join(table1: String, c1: String, table2: String, c2: String): Option[Table] = {
    //  Unwrap content from container
    def extractOptionDB(db: Option[Database]): Database =
      db match
        case Some(content) => content
        case None => Database(List.empty[Table])

    //  Extract the database from the option container, then the tables themselves
    val db = extractOptionDB(selectTables(List(table1, table2)))
    val (t1, t2): (Table, Table) = (db.tables(0), db.tables(1))

    //  Check exceptions before starting a join
    if (None.contains(t1)) return None
    else if (None.contains(t2)) return None
    else if (t1.tableData.isEmpty) return Some(t2)
    else if (t2.tableData.isEmpty) return Some(t1)

    //  Create 2 empty rows for when we concat a row without a match
    //  Give the same name to the joining columns
    val emptyRow1 = t1.tableData.head.map((k, v) => (k, ""))
    val emptyRow2 = t2.tableData.head.map((k, v) => (k, "")) - (c2, "") + ((c1, ""))

    //  Add keys from first row; if any of its keys are common, concat values
    //  Then add only the unique keys from second row
    def joinRows(r1: Row, r2: Row, col: String): Row = {
      def r1Cond(k: String, v: String): (String, String) =
        if (r2.contains(k))
          if (r1(k) == "" && r2(k) != "") (k, r2(k))
          else if (r1(k) != "" && r2(k) == "") (k, v)
          else if (r1(k) != "" && r2(k) != "" && r1(k) != r2(k)) (k, v + ';' + r2(k))
          else (k, v)
        else (k, v)

      r1.map((k, v) => r1Cond(k, v)) ++
        r2.foldRight(Map.empty[String, String])((x, xs)
        => if (!r1.contains(x._1) && x._1 != col) xs + x else xs)
    }

    //  Search for a row with the same key -> value as your wanted row
    @tailrec
    def findCorrespondentRow(key: String, value: String, t: Tabular): Row = {
      t match
        case head :: tail =>
          if (head(key) == value) head
          else findCorrespondentRow(key, value, tail)
        case Nil => Map.empty[String, String]
    }

    //  Parses table (the first given one) and adds
    //  Common rows within tables
    def parseTableForJoin(t1: Tabular, t2: Tabular): Tabular =
      t1 match
        case head :: tail =>
          val correspondentRow = findCorrespondentRow(c1, head(c1), t2)

          if (correspondentRow.isEmpty) parseTableForJoin(tail, t2)
          else joinRows(head, correspondentRow, c1) :: parseTableForJoin(tail, t2)

        case Nil => Nil

    //  Only adds rows without correspondent in another table
    def parseForUniqueRows(t1: Tabular, t2: Tabular)
                          (col: String, emptyRow: Row): Tabular = {
      t1 match
        case head :: tail =>
          val correspondentRow = findCorrespondentRow(c1, head(c1), t2)

          if (correspondentRow.isEmpty)
            joinRows(head, emptyRow, col) :: parseForUniqueRows(tail, t2)(col, emptyRow)
          else parseForUniqueRows(tail, t2)(col, emptyRow)

        case Nil => Nil
    }

    //  EXECUTION STARTS HERE

    //  Change common key from t2 to be identical to t1
    //  This way the merge is easier
    val t1Data = t1.tableData
    val t2Data = t2.tableData.map((r: Row) => r.map((k, v) => if (k == c2) (c1, v) else (k, v)))

    //  Execute join
    val commonRows: Tabular = parseTableForJoin(t1Data, t2Data)
    val completeTabular: Tabular =
      commonRows ::: parseForUniqueRows(t1Data, t2Data)(c1, emptyRow2)
        ::: parseForUniqueRows(t2Data, t1Data)(c1, emptyRow1)

    Option(Table(table1, completeTabular))
  }
}

//  Database indexing
extension (db: Database) {
  def apply(i: Int): Table = db.tables(i)
}