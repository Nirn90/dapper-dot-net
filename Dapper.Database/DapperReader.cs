using System;
using System.Collections.Generic;
using System.Data;
using System.Linq.Expressions;
using System.Linq;
using System.Reflection;

namespace Dapper
{
	public class TypeRecord<T> : IDataRecord
	{
		static protected List<PropertyInfo> _memberNames;
		static private List<Func<T, object>> _getters;

		static TypeRecord()
		{
			_getters = new List<Func<T, object>>();
			_memberNames = ReflectionHelper.GetSettableProps(typeof (T));
			foreach (var column in _memberNames)
			{
				var parameter = Expression.Parameter(typeof(T));
				var expr = Expression.TypeAs(Expression.Property(parameter, column.Name), typeof(object));
				var getter = Expression.Lambda<Func<T, object>>(expr, parameter).Compile();
				_getters.Add(getter);
			}
		}

		protected T _obj;

		public TypeRecord(T obj)
		{
			_obj = obj;
		}

		#region IDataRecord Members

		public int FieldCount
		{
			get { return _getters.Count; }
		}

		public bool GetBoolean(int i)
		{
			throw new NotImplementedException();
		}

		public byte GetByte(int i)
		{
			throw new NotImplementedException();
		}

		public long GetBytes(int i, long fieldOffset, byte[] buffer, int bufferoffset, int length)
		{
			throw new NotImplementedException();
		}

		public char GetChar(int i)
		{
			throw new NotImplementedException();
		}

		public long GetChars(int i, long fieldoffset, char[] buffer, int bufferoffset, int length)
		{
			throw new NotImplementedException();
		}

		public IDataReader GetData(int i)
		{
			throw new NotImplementedException();
		}

		public string GetDataTypeName(int i)
		{
			throw new NotImplementedException();
		}

		public DateTime GetDateTime(int i)
		{
			throw new NotImplementedException();
		}

		public decimal GetDecimal(int i)
		{
			throw new NotImplementedException();
		}

		public double GetDouble(int i)
		{
			throw new NotImplementedException();
		}

		public Type GetFieldType(int i)
		{
			throw new NotImplementedException();
		}

		public float GetFloat(int i)
		{
			throw new NotImplementedException();
		}

		public Guid GetGuid(int i)
		{
			throw new NotImplementedException();
		}

		public short GetInt16(int i)
		{
			throw new NotImplementedException();
		}

		public int GetInt32(int i)
		{
			throw new NotImplementedException();
		}

		public long GetInt64(int i)
		{
			throw new NotImplementedException();
		}

		public string GetName(int i)
		{
			throw new NotImplementedException();
		}

		public int GetOrdinal(string name)
		{
			return _memberNames.Select((m, i) => Tuple.Create(m.Name, i)).Single(t => t.Item1 == name).Item2;
		}

		public string GetString(int i)
		{
			throw new NotImplementedException();
		}

		public object GetValue(int i)
		{
			return _getters[i](_obj) ?? DBNull.Value;
		}

		public int GetValues(object[] values)
		{
			for (int i = 0; i < _getters.Count; i++)
				values[i] = _getters[i](_obj) ?? DBNull.Value;
			return _getters.Count;
		}

		public bool IsDBNull(int i)
		{
			throw new NotImplementedException();
		}

		public object this[string name]
		{
			get { throw new NotImplementedException(); }
		}

		public object this[int i]
		{
			get { throw new NotImplementedException(); }
		}

		#endregion
	}

	public class DapperReader<T> : TypeRecord<T>, IDataReader
	{
		IEnumerable<T> _data;
		IEnumerator<T> _enumerator;

		public DapperReader(IEnumerable<T> list): base(default(T))
		{
			_data = list;
			_enumerator = _data.GetEnumerator();
		}

		public DataTable ReadToDataTable()
		{
			DataTable dataTable = new DataTable();
			foreach (var member in _memberNames)
			{
				dataTable.Columns.Add(member.Name, member.PropertyType);
			}

			object[] values = new object[FieldCount];
			while (Read())
			{
				GetValues(values);
				dataTable.Rows.Add(values);
			}
			Close();
			return dataTable;
		}

		#region IDataReader Members

		public void Close()
		{
			this.Dispose();
		}

		public int Depth
		{
			get { return 1; }
		}

		public DataTable GetSchemaTable()
		{
			return null;
		}

		public bool IsClosed
		{
			get { throw new NotImplementedException(); }
		}

		public bool NextResult()
		{
			return false;
		}

		public bool Read()
		{
			if (_enumerator.MoveNext() == false) return false;
			_obj = _enumerator.Current;
			return true;
		}

		public int RecordsAffected
		{
			get { return -1; }
		}

		#endregion

		#region IDisposable Members

		public void Dispose()
		{
			_enumerator.Dispose();
		}

		#endregion
	}
}
