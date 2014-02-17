using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;

namespace Dapper
{
	public interface IEntityMemberMap : SqlMapper.IMemberMap
	{
		string FullDbColumnName { get; }
		bool IsIdentity { get; }
	}

	class EntityMemberMap : IEntityMemberMap
	{
		private readonly string _dbColumnName;
		private readonly string _dbFullColumnName;
		private readonly PropertyInfo _property;
		private readonly bool _isIdentity;

		public EntityMemberMap(string columnName, string fullColumnName, PropertyInfo property, bool isIdentity)
		{
			if (columnName == null)
				throw new ArgumentNullException("columnName");

			if (property == null)
				throw new ArgumentNullException("property");

			_dbColumnName = columnName;
			_dbFullColumnName = fullColumnName;
			_property = property;
			_isIdentity = isIdentity;
		}

		public FieldInfo Field { get { return null; } }
		public ParameterInfo Parameter { get { return null; } }

		public PropertyInfo Property { get { return _property; } }

		public Type MemberType { get { return _property.PropertyType; } }
		public string ColumnName { get { return _dbColumnName; } }
		public string FullDbColumnName { get { return _dbFullColumnName; } }
		public bool IsIdentity { get { return _isIdentity; } }
	}
}

