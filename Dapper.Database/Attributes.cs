using System;

namespace Dapper
{
	public class Column : Attribute
	{
		public string ColumnName;
		public bool IsIdentity;

		public Column(string columnName, bool isIdentity = false)
		{
			ColumnName = columnName;
			IsIdentity = isIdentity;
		}
	}

	public class Table : Attribute
	{
		public string TableName;

		public Table(string tableName)
		{
			TableName = tableName;
		}
	}

	public class Navigation : Attribute
	{
		public string SourceColumn;
		public string TargetColumn;

		public Navigation(string sourceColumn, string targetColumn)
		{
			SourceColumn = sourceColumn;
			TargetColumn = targetColumn;
		}
	}
}
