using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Data;
using System.Linq;
using System.Reflection;
using System.Reflection.Emit;
using System.Text;

namespace Dapper
{
	class DictionaryHolder
	{
		public Dictionary<int, object>[] Dictionaries;
		public Func<IDataReader, object>[] Deserializers;
		int prev_id;
		object prev_obj;

		public bool TryGetValue(int id, out object obj)
		{
			obj = prev_obj;
			return id == prev_id;
		}

		public void Add(int id, object obj)
		{
			prev_id = id;
			prev_obj = obj;
		}

		public DictionaryHolder(Type[] types, IDataReader rdr)
		{
			int count = types.Length - 1;
			Dictionaries = new Dictionary<int, object>[count];
			for (int i = 0; i < count; i++)
				Dictionaries[i] = new Dictionary<int, object>();
			Deserializers = SqlMapper.MultiMap(types, "Id", rdr);
		}
	}

	public interface ISqlQuery<T> : IEnumerable<T>
	{
		ISqlQuery<T> Include<TType>();
		ISqlQuery<T> Where(string sql, dynamic parameters = null);
	}

	public class SqlQuery<T> : ISqlQuery<T>
	{
		protected DbWorker Worker;
		protected List<Type> IncludedTypes;
		protected DynamicParameters Parameters;
		private string _where;

		protected SqlQuery(DbWorker worker, string where)
		{
			_where = where;
			Worker = worker;
			IncludedTypes = new List<Type>();
			Parameters = new DynamicParameters();
		}

		public SqlQuery(DbWorker worker)
		{
			Worker = worker;
			IncludedTypes = new List<Type>();
			Parameters = new DynamicParameters();
		}

		private SqlQuery(SqlQuery<T> query, string newWhere, Type type, dynamic parameters)
		{
			Worker = query.Worker;
			_where = newWhere;
			IncludedTypes = query.IncludedTypes.ToList();
			Parameters = new DynamicParameters(query.Parameters);
			Parameters.AddDynamicParams(parameters);
			if (type != null)
				IncludedTypes.Add(type);
		}

		public string GenSql()
		{
			var tm = Worker.Base.GetEntityMap(typeof(T));
			string select = tm.Identity.FullDbColumnName;
			select += "," + string.Join(",", tm.NonIdentityColumns.Select(s => s.FullDbColumnName));
			string from = tm.DbTableName;
			foreach (var inc in IncludedTypes)
			{
				var nav = tm.ForeignKeyDescriptions.Single(s => s.TargetMap.Type == inc);
				select += "," + string.Join(",", nav.TargetMap.DbColumns.Select(s => s.FullDbColumnName));
				from += " join " + nav.TargetMap.DbTableName + " on " + nav.SourceColumn.FullDbColumnName + "=" + nav.TargetColumn.FullDbColumnName;
			}

			StringBuilder sql = new StringBuilder("select ");
			sql.Append(select);
			sql.Append(" from ");
			sql.Append(from);
			if (!string.IsNullOrEmpty(_where))
			{
				sql.Append(" where ");
				sql.Append(_where);
			}
			return sql.ToString();
		}

		private IEnumerable<T> Execute()
		{
			if (IncludedTypes.Count == 0)
				return Worker.Connection.Query<T>(GenSql(), Parameters);
			else
			{
				return Query();
			}
		}

		IEnumerator IEnumerable.GetEnumerator()
		{
			return GetEnumerator();
		}

		public IEnumerator<T> GetEnumerator()
		{
			return Execute().GetEnumerator();
		}

		private IEnumerable<T> Query()
		{
			var cmd = SqlMapper.SetupDbCommand<T>(Worker.Connection, GenSql(), Parameters, null, null, null);
			//cmd.CommandText = GenSql();
			var rdr = cmd.ExecuteReader();
			var dm = GetDynamicDelegate();
			//var des = Dapper.SqlMapper.MultiMap(new Type[] { typeof(T) }.Concat(IncludedTypes).ToArray(), "Id", rdr);
			var dictionaryHolder = new DictionaryHolder(new Type[] { typeof(T) }.Concat(IncludedTypes).ToArray(), rdr);
			var del = (Func<IDataReader, T>)dm.CreateDelegate(typeof(Func<IDataReader, T>), dictionaryHolder);
			try
			{
				while (rdr.Read())
					yield return (T)del(rdr);
			}
			finally
			{
				rdr.Dispose();
			}
		}

		private IEnumerable<T> Read(IDataReader reader, Func<IDataReader, T> deserializer)
		{
			while (reader.Read())
				yield return (T)deserializer(reader);
			reader.Close();
		}

		public ISqlQuery<T> Where(string sql, dynamic parameters = null)
		{
			string newWhere;
			if (string.IsNullOrEmpty(_where))
				newWhere = sql;
			else
				newWhere = _where + " and " + sql;
			return new SqlQuery<T>(this, newWhere, null, parameters);
		}

		public ISqlQuery<T> Include<TType>()
		{
			return new SqlQuery<T>(this, _where, typeof(TType), null);
		}

		protected DynamicMethod GetDynamicDelegate()
		{
            var map = Worker.Base.GetEntityMap(typeof(T));
			DynamicMethod dm = new DynamicMethod("", typeof(T), new Type[] { typeof(DictionaryHolder), typeof(IDataReader) }.ToArray(), typeof(DictionaryHolder));
			var gn = dm.GetILGenerator();
			// Dictionary<int, object> dict;
			gn.DeclareLocal(typeof(Dictionary<int, object>));
			gn.DeclareLocal(typeof(T));
			gn.DeclareLocal(typeof(int));

			var clm = map.DbColumns.ToList();
			int index = clm.IndexOf(map.Identity);
			int bs = clm.Count;

			var getItem = typeof(IDataRecord).GetProperties().Where(p => p.GetIndexParameters().Any() && p.GetIndexParameters()[0].ParameterType == typeof(int)).First().GetGetMethod();

			gn.Emit(OpCodes.Ldarg_1);
			gn.Emit(OpCodes.Ldc_I4, index);
			gn.EmitCall(OpCodes.Callvirt, getItem, null);
			gn.Emit(OpCodes.Stloc_2);

			gn.Emit(OpCodes.Ldarg_0);
			gn.Emit(OpCodes.Ldloc_2);
			gn.Emit(OpCodes.Ldloca, 1);
			gn.EmitCall(OpCodes.Call, typeof(DictionaryHolder).GetMethod("TryGetValue"), null);
			var lbl = gn.DefineLabel();
			gn.Emit(OpCodes.Brtrue, lbl);

			gn.Emit(OpCodes.Ldarg_0);
			gn.Emit(OpCodes.Ldloc_2);
			gn.Emit(OpCodes.Ldarg_0);
			gn.Emit(OpCodes.Ldfld, typeof(DictionaryHolder).GetField("Deserializers"));
			gn.Emit(OpCodes.Ldc_I4, 0);
			gn.Emit(OpCodes.Ldelem, typeof(DictionaryHolder));
			gn.Emit(OpCodes.Ldarg_1);
			gn.EmitCall(OpCodes.Call, typeof(Func<IDataReader, object>).GetMethod("Invoke"), null);
			gn.Emit(OpCodes.Dup);
			gn.Emit(OpCodes.Stloc_1);
			gn.EmitCall(OpCodes.Call, typeof(DictionaryHolder).GetMethod("Add"), null);
			gn.MarkLabel(lbl);

			for (int i = 0; i < IncludedTypes.Count; i++)
			{
				var nav = map.ForeignKeyDescriptions.Single(n => n.TargetMap.Type == IncludedTypes[i]);

				//IncludedTypes[i] obj
				gn.DeclareLocal(IncludedTypes[i]);

				clm = nav.TargetMap.DbColumns.ToList();
				index = bs + clm.IndexOf(nav.TargetMap.Identity);
				bs += clm.Count;

				gn.Emit(OpCodes.Ldarg_1);
				gn.Emit(OpCodes.Ldc_I4, index);
				gn.EmitCall(OpCodes.Callvirt, getItem, null);
				gn.Emit(OpCodes.Stloc_2);

				gn.Emit(OpCodes.Ldarg_0);
				gn.Emit(OpCodes.Ldfld, typeof(DictionaryHolder).GetField("Dictionaries"));
				gn.Emit(OpCodes.Ldc_I4, i);
				gn.Emit(OpCodes.Ldelem, typeof(DictionaryHolder));
				gn.Emit(OpCodes.Dup);
				gn.Emit(OpCodes.Stloc_0);

				gn.Emit(OpCodes.Ldloc_2);   // [dict][dict][ind]
				gn.Emit(OpCodes.Ldloca, i + 3); // [dict][dict][ind][&obj]
				gn.EmitCall(OpCodes.Call, typeof(Dictionary<int, object>).GetMethod("TryGetValue"), null);  //[dict][bool]
				lbl = gn.DefineLabel();
				gn.Emit(OpCodes.Brtrue, lbl);   //[dict]

				gn.Emit(OpCodes.Ldloc_0);
				gn.Emit(OpCodes.Ldloc_2);   //[dict][ind]
				gn.Emit(OpCodes.Ldarg_0);   //[dict][ind][this]
				gn.Emit(OpCodes.Ldfld, typeof(DictionaryHolder).GetField("Deserializers")); //[dict][ind][des]
				gn.Emit(OpCodes.Ldc_I4, i + 1); // [dict][ind][des][des_ind]
				gn.Emit(OpCodes.Ldelem, typeof(DictionaryHolder));  // [dict][ind][de]
				gn.Emit(OpCodes.Ldarg_1);   // [dict][ind][de][rdr]
				gn.EmitCall(OpCodes.Call, typeof(Func<IDataReader, object>).GetMethod("Invoke"), null); // [dict][ind][obj]
				gn.Emit(OpCodes.Dup);   // [dict][ind][obj][obj]
				gn.Emit(OpCodes.Stloc, i + 3);  // [dict][ind][obj]
				gn.EmitCall(OpCodes.Call, typeof(Dictionary<int, object>).GetMethod("Add"), null);  // empty
				gn.MarkLabel(lbl);

				if (!nav.OtM)
				{
					gn.Emit(OpCodes.Ldloc_1);
					gn.Emit(OpCodes.Ldloc, i + 3);
					gn.EmitCall(OpCodes.Call, nav.Property.GetSetMethod(), null);
				}
				else
				{
					gn.Emit(OpCodes.Ldloc_1);
					gn.EmitCall(OpCodes.Call, nav.Property.GetGetMethod(), null);
					gn.Emit(OpCodes.Ldloc, i + 3);
					gn.EmitCall(OpCodes.Callvirt, typeof(ICollection<>).GetMethod("Add"), null);
				}
			}

			gn.Emit(OpCodes.Ldloc_1);
			gn.Emit(OpCodes.Ret);
			return dm;
		}
	}    
}
