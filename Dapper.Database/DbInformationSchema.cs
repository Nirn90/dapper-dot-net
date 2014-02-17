using System;
using System.Collections.Generic;
using System.Linq;

namespace Dapper
{
	public class DbInformationSchema
	{
		private Dictionary<string, List<string>> _schema;
		private Dictionary<Type, IEntityMap> _maps;

		public DbInformationSchema()
		{
			_schema = new Dictionary<string, List<string>> ();
			_maps = new Dictionary<Type, IEntityMap> ();
		}

		public void AddDbTable(string tableName, IEnumerable<string> columns)
		{
			_schema.Add (tableName, columns.ToList ());
		}

		public void AddEntityMap(Type type, IEntityMap map)
		{
			_maps.Add (type, map);
		}

		public IEntityMap GetDbTypeMap(Type type)
		{
			return _maps [type];
		}

		public List<string> GetTableColumns(string tableName)
		{
			return _schema[tableName];
		}

		public bool TableExists(string tableName)
		{
			return _schema.ContainsKey(tableName);
		}
	}
}

