type Row = Map[String, String]
type Tabular = List[Row]

case class Table (tableName: String, tableData: Tabular) {

  override def toString: String = {
    //  Put elements of a row consecutively, with a ',' in between
    //  c represents operating on a header or row
    def mapToString(row: Row, keys: Iterable[String])
                   (c: Char): String =
      if (c == 'h')
        keys.foldRight("")((x, xs) =>
          if (xs == "") x + xs
          else x + ',' + xs)
      else
        val str = keys.foldLeft("")((xs, x) => xs + row(x) + ',')
        str.substring(0, str.length - 1)


    //  Keys + newline + all rows, formatted as CSV
    val result = mapToString(data.head, header)('h') + '\n' +
      data.foldLeft("")((xs, x) => xs + mapToString(x, header)('c') + '\n')

    //  Remove last newline
    result.substring(0, result.length - 1)
  }

  //  Add element to data
  def insert(row: Row): Table =
    if (!data.contains(row)) new Table(name, data ++ List(row))
    else new Table(name, data)

  //  Delete element from data by filtering
  def delete(row: Row): Table = {
    val newData = data.filter(x => x != row)

    new Table(name, newData)
  }

  //  Sort by column
  def sort(column: String): Table = {
    val sortedData = data.sortBy(_(column))

    new Table(name, sortedData)
  }

  def update(f: FilterCond, updates: Map[String, String]): Table = {
    val newData = data.map(r =>
      //  Evaluate each row
      f.eval(r) match
        //  If row is eligible, update required columns
        case Some(true) => r.map(x => if (updates.contains(x._1)) x._1 -> updates(x._1) else x)
        //  Else, just keep the row unmodified
        case _ => r)

    new Table(name, newData)
  }

  def filter(f: FilterCond): Table = {
    val newData = data.filter(r =>
      f.eval(r) match
        case Some(statement) => statement
        case None => false)

    new Table(name, newData)
  }

  //  For each row, remove columns absent from the list
  def select(columns: List[String]): Table = {
    val removables = header.diff(columns)
    val newData = data.foldRight(List.empty[Row])((x, xs) => (x -- removables) :: xs)

    new Table(name, newData)
  }

  def header: List[String] = data.head.keys.toList
  def data: Tabular = tableData
  def name: String = tableName
}

object Table {
  def apply(name: String, s: String): Table = {
    val arr = s.split('\n').toList.map(_.split(',').toList)
    val columns = arr.head
    val info = arr.tail

    //  For every row, create a tuple between keys and values then map them
    def mapRows(info: List[List[String]]): Tabular = {
      def mapElements(row: List[String]): Row =
        columns.zip(row).toMap

      info match
        case head :: tail => mapElements(head) :: mapRows(tail)
        case Nil => Nil
    }

    val newData = mapRows(info)

    new Table(name, newData)
  }
}

//  Table indexing
extension (table: Table) {
  def apply(i: Int): Table = new Table(table.tableName, List(table.tableData(i)))
}
