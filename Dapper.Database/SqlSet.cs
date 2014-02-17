using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Text;

namespace Dapper
{
	public class SqlSet<T> : SqlQuery<T>
	{
		protected SqlSet(DbWorker worker, string where) : base(worker, where) { }

		public SqlSet(DbWorker worker)
			: base(worker)
		{
		}

		public virtual int Insert(T obj)
		{
			var map = Worker.Base.GetEntityMap(typeof(T));
			StringBuilder sql = new StringBuilder();
			sql.Append("insert ");
			sql.Append(map.DbTableName);
			sql.Append(" (");
			sql.Append(string.Join(",", map.NonIdentityColumns.Select(p => p.ColumnName)));
			sql.Append(") values (");
			sql.Append(string.Join(",", map.NonIdentityColumns.Select(p => "@" + p.ColumnName)));
			sql.Append(") select cast(scope_identity() as int)");
			return Worker.Connection.Query<int>(sql.ToString(), obj).Single();
		}

		public virtual void Insert(IEnumerable<T> list)
		{
			var map = Worker.Base.GetEntityMap(typeof(T));
			StringBuilder sql = new StringBuilder();
			sql.Append("insert ");
			sql.Append(map.DbTableName);
			sql.Append(" (");
			sql.Append(string.Join(",", map.NonIdentityColumns.Select(p => p.ColumnName)));
			sql.Append(") values (");
			sql.Append(string.Join(",", map.NonIdentityColumns.Select(p => "@" + p.ColumnName)));
			sql.Append(") select cast(scope_identity() as int)");
			foreach (var obj in list)
			{
				int i = Worker.Connection.Query<int>(sql.ToString(), obj).Single();
				typeof(T).GetProperty("Id").SetValue(obj, i, null);
			}
		}

		public virtual void Update(T obj)
		{
			var map = Worker.Base.GetEntityMap(typeof(T));
			StringBuilder sql = new StringBuilder();
			sql.Append("update ");
			sql.Append(map.DbTableName);
			sql.Append(" set ");
			sql.Append(string.Join(",", map.NonIdentityColumns.Select(p => p.ColumnName + "=@" + p.ColumnName)));
			sql.Append(" where ");
			sql.Append(map.Identity.ColumnName + "=@" + map.Identity.ColumnName);
			Worker.Connection.Execute(sql.ToString(), obj);
		}

		public virtual void Update(IEnumerable<T> list)
		{
			var map = Worker.Base.GetEntityMap(typeof(T));
			StringBuilder sql = new StringBuilder();
			sql.Append("update ");
			sql.Append(map.DbTableName);
			sql.Append(" set ");
			sql.Append(string.Join(",", map.NonIdentityColumns.Select(p => p.ColumnName + "=@" + p.ColumnName)));
			sql.Append(" where ");
			sql.Append(map.Identity.ColumnName + "=@" + map.Identity.ColumnName);
			Worker.Connection.Execute(sql.ToString(), list);
		}

		public virtual void Remove(T obj)
		{
			var map = Worker.Base.GetEntityMap(typeof(T));
			StringBuilder sql = new StringBuilder();
			sql.Append("delete ");
			sql.Append(map.DbTableName);
			sql.Append(" where ");
			sql.Append(map.Identity.ColumnName + "=@" + map.Identity.ColumnName);
			Worker.Connection.Execute(sql.ToString(), obj);
		}

		public virtual void Remove(IEnumerable<T> list)
		{
			var map = Worker.Base.GetEntityMap(typeof(T));
			StringBuilder sql = new StringBuilder();
			sql.Append("delete ");
			sql.Append(map.DbTableName);
			sql.Append(" where ");
			sql.Append(map.Identity.ColumnName + "=@" + map.Identity.ColumnName);
			Worker.Connection.Execute(sql.ToString(), list);
		}

		public void BulkInsert(IEnumerable<T> list, string destTableName, int batchSize)
		{
			var map = Worker.Base.GetEntityMap(typeof(T));
			DapperReader<T> dapperReader = new DapperReader<T>(list);
			using (var bulk = new SqlBulkCopy((SqlConnection)Worker.Connection))
			{
				bulk.DestinationTableName = destTableName;

				foreach (var column in map.NonIdentityColumns)
					bulk.ColumnMappings.Add(column.Property.Name, column.ColumnName);

				bulk.BatchSize = batchSize;
				bulk.WriteToServer(dapperReader);
			}
		}
	}
}

