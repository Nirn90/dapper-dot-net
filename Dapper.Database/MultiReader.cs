using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;

namespace Dapper
{
	public class MultiReader : IDisposable
	{
		DbDataReader _reader;

		public MultiReader(DbDataReader reader)
		{
			_reader = reader;
		}

		public IEnumerable<IDataRecord> Read()
		{
			while (_reader.Read())
				yield return _reader;
		}

		public IEnumerable<T> Read<T>(Func<DbDataReader, T> deserializer)
		{
			while (_reader.Read())
				yield return deserializer(_reader);
		}

		public IEnumerable<T> DapperRead<T>()
		{
			var deserializer = Dapper.SqlMapper.GetTypeDeserializer(typeof(T), _reader);
			while (_reader.Read())
				yield return (T)deserializer(_reader);
		}

		public IEnumerable<IDataRecord> ReadNext()
		{
			if (_reader.NextResult() == false)
				return null;
			return Read();
		}

		public IEnumerable<T> ReadNext<T>(Func<DbDataReader, T> deserializer)
		{
			if (_reader.NextResult() == false)
				return null;
			return Read(deserializer);
		}

		public IEnumerable<T> DapperReadNext<T>()
		{
			if (_reader.NextResult() == false)
				return null;
			return DapperRead<T>();
		}

		public void Dispose()
		{
			_reader.Close();
		}
	}
}
