using System.Reflection;
using System.Collections.Generic;
using System;
using System.Linq;

namespace Dapper
{
	static class ReflectionHelper
	{
		private static MethodInfo GetPropertySetter(PropertyInfo propertyInfo, Type type)
		{
			return propertyInfo.DeclaringType == type ?
				propertyInfo.GetSetMethod(true) :
				propertyInfo.DeclaringType.GetProperty(propertyInfo.Name, BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance).GetSetMethod(true);
		}

		public static List<PropertyInfo> GetSettableProps(Type t)
		{
			return t
					.GetProperties(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance)
					.Where(p => GetPropertySetter(p, t) != null)
					.ToList();
		}
	}
}
