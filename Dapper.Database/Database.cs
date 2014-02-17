using System;
using System.Data.Common;
using System.Data.SqlClient;
using System.Linq;
using System.Data;
using System.Collections.Generic;

namespace Dapper
{
	public abstract class DbBase
	{
		protected DbInformationSchema _schema;

		public string ConnectionString { get; private set; }

		protected DbBase()
		{
		}

		protected DbBase(string cstr)
		{
			ConnectionString = cstr;
		}

		public void Initialize()
		{
			_schema = GetInformationSchema ();
		}

		public abstract DbConnection CreateConnection();

		protected abstract DbInformationSchema GetInformationSchema ();

		protected void RegisterEntityGraph(Type type)
		{
			EntityMapper mapper = new EntityMapper (type, _schema);
			mapper.Map ();
		}

		public IEntityMap GetEntityMap(Type type)
		{
			return _schema.GetDbTypeMap (type);
		}
	}

	public class SqlDbBase : DbBase
	{
		public SqlDbBase()
		{
		}

		public SqlDbBase(string cstr)
			: base(cstr)
		{

		}

		public override DbConnection CreateConnection()
		{
			return new SqlConnection(ConnectionString);
		}

		protected override DbInformationSchema GetInformationSchema ()
		{
			DbInformationSchema schema = new DbInformationSchema ();
			string sql = "select t.name,c.name from sys.tables t join sys.columns c on t.object_id=c.object_id";
			using (var wrk = new DbWorker(this))
			{
				var data = wrk.Select (sql).GroupBy (r => r.GetString (0), r => r.GetString (1));
				foreach (var grouping in data)
					_schema.AddDbTable (grouping.Key, grouping);
			}
			return schema;
		}

		/*public bool IsLocal()
		{
			var builder = new SqlConnectionStringBuilder(ConnectionString);
			using (var wrk = new DbWorker(this))
			{
				string name = wrk.Connection.Query("exec sp_helpserver").Single().name;
				string mname = name.Substring(0, name.IndexOf('\\'));
				return mname == Environment.MachineName;
			}
		}*/
	}

	public class DbWorker : IDisposable
	{
		protected readonly DbConnection _connection;
		public DbConnection Connection { get { return _connection; } }
		public DbBase Base { get; private set; }

		public DbWorker(DbBase db)
		{
			Base = db;
			_connection = db.CreateConnection();
			_connection.Open();
		}

		public DbWorker(DbConnection dbcon)
		{
			_connection = dbcon;
			_connection.Open();
		}

		public DbCommand CreateCommand()
		{
			return _connection.CreateCommand();
		}

		public SqlSet<T> Set<T>()
		{
			return new SqlSet<T>(this);
		}

		public IEnumerable<IDataRecord> Select(string query, params DbParameter[] prs)
		{
			var cmd = CreateCommand();
			cmd.CommandText = query;
			cmd.Parameters.AddRange(prs);
			using (var dr = cmd.ExecuteReader())
				while (dr.Read())
					yield return dr;
		}

		public IEnumerable<T> Select<T>(string query, Func<DbDataReader, T> rfunc, params DbParameter[] prs)
		{
			var cmd = CreateCommand();
			cmd.CommandText = query;
			cmd.Parameters.AddRange(prs);
			using (var dr = cmd.ExecuteReader())
				while (dr.Read())
					yield return rfunc(dr);
		}

		public MultiReader SelectMultiple(string query)
		{
			var cmd = CreateCommand();
			cmd.CommandText = query;
			return new MultiReader(cmd.ExecuteReader());
		}

		public IEnumerable<IDataRecord> Select<T>(string query, IEnumerable<T> source, Action<DbParameterCollection, T> setter, params DbParameter[] prs)
		{
			var cmd = CreateCommand();
			cmd.CommandText = query;
			cmd.Parameters.AddRange(prs);
			foreach (var obj in source)
			{
				setter(cmd.Parameters, obj);
				using (var dr = cmd.ExecuteReader())
					while (dr.Read())
						yield return dr;
			}
		}

		public void Update<T>(string query, IEnumerable<T> source, Action<DbParameterCollection, T> setter, params DbParameter[] prs)
		{
			var cmd = CreateCommand();
			cmd.CommandText = query;
			cmd.Parameters.AddRange(prs);
			using (var trans = _connection.BeginTransaction())
			{
				cmd.Transaction = trans;
				foreach (var obj in source)
				{
					setter(cmd.Parameters, obj);
					cmd.ExecuteNonQuery();
				}
				trans.Commit();
			}
		}

		public void Dispose()
		{
			_connection.Close();
		}
	}
}

