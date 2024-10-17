object Queries {
  //  Filter table by name column
  def killJackSparrow(t: Table): Option[Table] =
    queryT((Some(t), "FILTER", Field("name", (str: String) => str != "Jack")))

  def insertLinesThenSort(db: Database): Option[Table] = {
//    val newDB = queryDB((Some(db), "CREATE", "Inserted Fellas"))
//    val crtTable = queryDB((newDB, "SELECT", List("Inserted Fellas"))) match
//      case Some(db) => db.tables.head
//      case None => return None
//
//    val updatedTable = queryT((Some(crtTable), "INSERT",
//      List(Map("name" -> "Ana", "age" -> "93", "CNP" -> "455550555"),
//        Map("name" -> "Diana", "age" -> "33", "CNP" -> "255532142"),
//        Map("name" -> "Tatiana", "age" -> "55", "CNP" -> "655532132"),
//        Map("name" -> "Rosmaria", "age" -> "12", "CNP" -> "855532172"))))
//
//    queryT((updatedTable, "SORT", "age"))

    //  Create table. Select it. Insert rows. Sort
    queryT((queryT((Some(queryDB((queryDB((Some(db), "CREATE", "Inserted Fellas")), "SELECT",
      List("Inserted Fellas"))) match {
        case Some(db) => db.tables.head
        case None => return None }), "INSERT",
      List(Map("name" -> "Ana", "age" -> "93", "CNP" -> "455550555"),
        Map("name" -> "Diana", "age" -> "33", "CNP" -> "255532142"),
        Map("name" -> "Tatiana", "age" -> "55", "CNP" -> "655532132"),
        Map("name" -> "Rosmaria", "age" -> "12", "CNP" -> "855532172")))), "SORT", "age"))
  }

  def youngAdultHobbiesJ(db: Database): Option[Table] = {
//    val table: Table = queryDB(PP_SQL_DB_Join(Some(db), "JOIN", "People", "name", "Hobbies", "name")) match
//      case Some(content) => content.tables.head
//      case None => return None
//
//    val conditions: List[FilterCond] = List(Field("age", _ < "25"), Field("name", _.head == 'J'),
//      Field("hobby", _.nonEmpty))
//    val filteredTable: Option[Table] = queryT(PP_SQL_Table_Filter(Some(table), "FILTER", All(conditions)))
//
//    filteredTable match
//      case Some(content) => Some(content.select(List("name", "hobby")))
//      case None => None

    //  Join tables. Filter by conditions. Return table with only selected columns.
    queryT((Some(queryDB((Some(db), "JOIN", "People", "name", "Hobbies", "name")) match
        case Some(content) => content.tables.head
        case None => return None),
      "FILTER", All(List(Field("age", _ < "25"), Field("name", _.head == 'J'),
      Field("hobby", _.nonEmpty))))) match
        case Some(content) => Some(content.select(List("name", "hobby")))
        case None => None
  }
}
