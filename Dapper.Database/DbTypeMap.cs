using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;

namespace Dapper
{
	public class ForeignKeyDescription
	{
		IEntityMap _targetMap;
		IEntityMemberMap _targetColumn;
		IEntityMemberMap _sourceColumn;
		PropertyInfo _property;
		bool _otm;

		public IEntityMap TargetMap { get { return _targetMap; } }
		public IEntityMemberMap TargetColumn { get { return _targetColumn; } }
		public IEntityMemberMap SourceColumn { get { return _sourceColumn; } }
		public PropertyInfo Property { get { return _property; } }
		public bool OtM { get { return _otm; } }

		public ForeignKeyDescription(IEntityMemberMap sourceColumn, IEntityMap targetMap, IEntityMemberMap targetColumn, PropertyInfo property, bool otm)
		{
			_sourceColumn = sourceColumn;
			_targetColumn = targetColumn;
			_targetMap = targetMap;
			_property = property;
			_otm = otm;
		}
	} 

	public interface IEntityMap : SqlMapper.ITypeMap
	{
		Type Type { get; }
		string DbTableName { get; }
		IEntityMemberMap Identity { get; }
		IEnumerable<IEntityMemberMap> DbColumns { get; }
		IEnumerable<ForeignKeyDescription> ForeignKeyDescriptions { get; }
		IEnumerable<IEntityMemberMap> NonIdentityColumns { get; }

		IEntityMemberMap FindMapByPropertyName(string propName);
	}

	public class EntityMap : IEntityMap
	{
		Type _type;
		string _dbTableName;
		List<SqlMapper.IMemberMap> _columns;
		List<ForeignKeyDescription> _foreignKeyDescriptions = new List<ForeignKeyDescription>();
		IEntityMemberMap _identity;

		public Type Type
		{
			get { return _type; }
		}

		public string DbTableName
		{
			get { return _dbTableName; }
		}

		public IEntityMemberMap Identity
		{
			get { return _identity; }
		}

		public IEnumerable<SqlMapper.IMemberMap> Columns
		{
			get { return _columns; }
		}

		public IEnumerable<IEntityMemberMap> DbColumns
		{
			get { return _columns.OfType<IEntityMemberMap>(); }
		}

		public IEnumerable<ForeignKeyDescription> ForeignKeyDescriptions
		{
			get { return _foreignKeyDescriptions; }
		}

		public IEnumerable<IEntityMemberMap> NonIdentityColumns
		{
			get { return DbColumns.Where(p => p.ColumnName != _identity.ColumnName); }
		}

		internal EntityMap(Type type, List<SqlMapper.IMemberMap> columns, string dbTableName,
			List<ForeignKeyDescription> foreignKeyDescriptions, IEntityMemberMap identity)
		{
			_columns = columns;
			_dbTableName = dbTableName;
			_type = type;
			_foreignKeyDescriptions = foreignKeyDescriptions;
			_identity = identity;
		}

		public IEntityMemberMap FindMapByPropertyName(string propName)
		{
			return DbColumns.FirstOrDefault(c => c.Property.Name == propName);
		}

		public ConstructorInfo FindConstructor(string[] names, Type[] types)
		{
			return _type.GetConstructor(new Type[0]);
		}

		public SqlMapper.IMemberMap GetConstructorParameter(ConstructorInfo constructor, string columnName)
		{
			throw new NotSupportedException();
		}

		public SqlMapper.IMemberMap GetMember(string columnName)
		{
			return _columns.FirstOrDefault(c => c.ColumnName == columnName);
		}
	}

	public class EntityMapper
	{
		private Type _type;
		private DbInformationSchema _dbInformationSchema;
		private List<PropertyInfo> _properties; 
		private List<string> _dbColumnNames;
		private List<SqlMapper.IMemberMap> _columns;
		private IEntityMemberMap _identity;
		private string _dbTableName;
		private List<ForeignKeyDescription> _foreignKeyDescriptions; 

		public EntityMapper(Type type, DbInformationSchema schema)
		{
			_type = type;
			_dbInformationSchema = schema;
			_properties = ReflectionHelper.GetSettableProps(_type);
			_columns = new List<SqlMapper.IMemberMap>();
			_foreignKeyDescriptions = new List<ForeignKeyDescription>();
		}

		public void Map()
		{
			MapTable();
			_dbColumnNames = _dbInformationSchema.GetTableColumns(_dbTableName);
			MapColumns();
			CreateNavigationProperties();

			var typeMap = new EntityMap(_type, _columns, _dbTableName, _foreignKeyDescriptions, _identity);
			_dbInformationSchema.AddEntityMap (_type, typeMap);
		}

		private void MapTable()
		{
			string sqlTableName = _type.Name;
			var attribs = _type.GetCustomAttributes(typeof(Table), true);
			if (attribs.Length > 0)
				sqlTableName = ((Table)attribs[0]).TableName;
			if (!_dbInformationSchema.TableExists(sqlTableName))
				throw new KeyNotFoundException("The table doesn't exist in the database");
			_dbTableName = sqlTableName;
		}

		private void MapColumns()
		{
			foreach (var prop in _properties)
			{
				_columns.Add(MapMember(prop));
			}
			_identity = _columns.OfType<IEntityMemberMap>().Single(m => m.IsIdentity);
		}

		private SqlMapper.IMemberMap MapMember(MemberInfo memberInfo)
		{
			string likelyDbColumnName = memberInfo.Name;
			bool isIdentity = false;
			var cAttribs = memberInfo.GetCustomAttributes(typeof(Column), true);
			if (cAttribs.Length > 0)
			{
				Column columnAttrib = (Column)cAttribs[0];
				likelyDbColumnName = columnAttrib.ColumnName;
				isIdentity = columnAttrib.IsIdentity;
			}
			if (_dbColumnNames.Contains(likelyDbColumnName))
				return new EntityMemberMap(likelyDbColumnName, _dbTableName + "." + likelyDbColumnName, (PropertyInfo)memberInfo, isIdentity);
			return new SimpleMemberMap(likelyDbColumnName, (PropertyInfo)memberInfo);
		}

		private void CreateNavigationProperties()
		{
			foreach (var prop in _properties)
			{
				var nvAttribs = prop.GetCustomAttributes(typeof(Navigation), true);
				if (nvAttribs.Length > 0)
				{
					var nav = (Navigation)nvAttribs[0];
					_foreignKeyDescriptions.Add(CreateNavigation(prop, nav));
				}
			}
		}

		private ForeignKeyDescription CreateNavigation(PropertyInfo propertyInfo, Navigation navAttrib)
		{
			ForeignKeyDescription description;
			Type targetType;
			var srcCol = _columns.OfType<IEntityMemberMap>().Single(c => c.ColumnName == navAttrib.SourceColumn);
			bool isCollection = propertyInfo.PropertyType == typeof(ICollection<>);
			if (isCollection)
				targetType = propertyInfo.PropertyType.GetGenericArguments()[0];
			else
				targetType = propertyInfo.PropertyType;
			EntityMapper mapper = new EntityMapper(targetType, _dbInformationSchema);
			mapper.Map ();
			var m = _dbInformationSchema.GetDbTypeMap(targetType);
			var trgCol = m.DbColumns.Single(c => c.ColumnName == navAttrib.TargetColumn);
			description = new ForeignKeyDescription(srcCol, m, trgCol, propertyInfo, isCollection);
			return description;
		}
	}
}

