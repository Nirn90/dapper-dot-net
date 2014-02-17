using System;
using System.Data;

namespace Dapper
{
    public partial class SqlMapper
    {
        public static Func<IDataReader, object>[] MultiMap(Type[] types, string splitOn, IDataReader reader)
        {
            return GenerateDeserializers(types, splitOn, reader);
        }

        public static IDbCommand SetupDbCommand<T>(IDbConnection cnn, string sql, object param, IDbTransaction transaction, int? commandTimeout, CommandType? commandType)
        {
            var identity = new Identity(sql, commandType, cnn, typeof(T), param == null ? null : param.GetType(), null);
            var info = GetCacheInfo(identity);
            return SetupCommand(cnn, transaction, sql, info.ParamReader, param, commandTimeout, commandType);
        }
    }  
}
