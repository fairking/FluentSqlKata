using Microsoft.EntityFrameworkCore;
using SqlKata;
using SqlKata.Compilers;
using System.Data;
using System.Data.Common;
using System.Linq.Expressions;
using System.Reflection;

namespace FluentSqlKata.EFCore6
{
    public static class DbContextHelper
    {
        #region Queries

        public static Query Query(this DbContext dbContext)
        {
            return FluentQuery.Query();
        }

        public static Query Query<A>(this DbContext dbContext)
        {
            return FluentQuery.Query<A>();
        }

        public static Query Query<A>(this DbContext dbContext, Expression<Func<A>> alias)
        {
            return FluentQuery.Query(alias);
        }

        #endregion Queries

        #region Execution

        public static async Task<T> ToScalar<T>(this DbContext dbContext, Query query, CancellationToken cancellationToken = default)
        {
            T result;

            using (var cmd = await dbContext.CreateDbCommand(query, cancellationToken))
            {
                // Read rows
                result = (T)await cmd.ExecuteScalarAsync(cancellationToken);
            }

            return result;
        }

        public static async Task<IEnumerable<T>> ToList<T>(this DbContext dbContext, Query query, CancellationToken cancellationToken = default) where T : class, new()
        {
            IEnumerable<T> result;

            using (var cmd = await dbContext.CreateDbCommand(query, cancellationToken))
            {
                // Read scalar value
                result = await cmd.ReadObjectsAsync<T>(cancellationToken);
            }

            return result;
        }

        #endregion Execution

        #region Private Methods

        private static async Task<DbCommand> CreateDbCommand(this DbContext dbContext, Query query, CancellationToken cancellationToken = default)
        {
            Compiler compiler;

            // Create a compiler
            if (dbContext.Database.IsSqlServer())
                compiler = new SqlServerCompiler();
            else if (dbContext.Database.IsSqlite())
                compiler = new SqliteCompiler();
            else
                throw new NotSupportedException($"The provided database context '{dbContext.GetType()}' is not supported to create a SqlKata compiler.");

            // Compile to sql query
            var sql = compiler.Compile(query);

            var cmd = dbContext.Database.GetDbConnection().CreateCommand();

            if (cmd.Connection.State == ConnectionState.Closed)
                await cmd.Connection.OpenAsync(cancellationToken);

            cmd.CommandText = sql.Sql;

            // Add query parameters
            foreach (var bind in sql.NamedBindings)
            {
                var p = cmd.CreateParameter();
                p.ParameterName = bind.Key;
                p.Value = bind.Value;
                cmd.Parameters.Add(p);
            }

            return cmd;
        }

        private static async Task<IEnumerable<T>> ReadObjectsAsync<T>(this DbCommand cmd, CancellationToken cancellationToken = default) where T : class, new()
        {
            // Execute query
            var reader = await cmd.ExecuteReaderAsync(cancellationToken);

            if (!reader.HasRows)
                return new T[0];

            var properties = typeof(T)
                .GetWritableProperties()
                .ToDictionary(x => x.Name, x => x);

            var result = new List<T>();

            // Read rows
            while (await reader.ReadAsync(cancellationToken))
            {
                var item = (T)Activator.CreateInstance(typeof(T), true);

                // Read columns
                for (int i = 0; i < reader.FieldCount; ++i)
                {
                    string columnName = reader.GetName(i);

                    if (properties.TryGetValue(columnName, out var mappedProperty))
                    {
                        var value = reader[columnName];

                        if (value is DBNull)
                            value = GetDefaultValue(mappedProperty.PropertyType);

                        try
                        {
                            if (value != null)
                                value = value.ConvertType(mappedProperty.PropertyType);

                            mappedProperty.SetValue(item, value);
                        }
                        catch (Exception exc)
                        {
                            throw new InvalidCastException($"Could not cast database type {value?.GetType().FullName} into the property type {mappedProperty.PropertyType.FullName} for property name {typeof(T).Name}.{mappedProperty.Name}", exc);
                        }
                    }
                }

                result.Add(item);
            }

            return result.ToArray();
        }

        private static object ConvertType(this object obj, Type to)
        {
            var from = obj.GetType().GetNullableUnderlyingType();

            var u_to = to.GetNullableUnderlyingType();

            if (from == u_to)
                return obj;

            // Sqlite has dates in string format
            if (u_to == typeof(DateTime) && from == typeof(string))
                return Convert.ToDateTime(obj);

            return Convert.ChangeType(obj, u_to);
        }

        private static PropertyInfo[] GetWritableProperties(this Type type)
        {
            return type.GetProperties(BindingFlags.Public | BindingFlags.Instance);
        }

        private static Type GetNullableUnderlyingType(this Type type)
        {
            if (type.IsGenericType && type.GetGenericTypeDefinition() == typeof(Nullable<>))
                return Nullable.GetUnderlyingType(type);
            else
                return type;
        }

        private static object GetDefaultValue(this Type t)
        {
            if (t.IsValueType && Nullable.GetUnderlyingType(t) == null)
                return Activator.CreateInstance(t);
            else
                return null;
        }

        #endregion Private Methods
    }
}