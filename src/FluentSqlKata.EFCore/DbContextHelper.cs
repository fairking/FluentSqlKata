using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Storage;
using SqlKata;
using SqlKata.Compilers;
using System.Collections;
using System.ComponentModel.DataAnnotations.Schema;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.CompilerServices;

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

        public static async Task<TResult> ToScalarAsync<TResult>(this DbContext dbContext, Query query, CancellationToken cancellationToken = default)
        {
            TResult result;

            var compiler = new SqlServerCompiler() { UseLegacyPagination = true };

            // Compile to sql query
            var sql = compiler.Compile(query);

#if DEBUG
            Debug.WriteLine(sql);
#endif

            using (var cmd = dbContext.Database.GetDbConnection().CreateCommand())
            {
                if (cmd.Connection.State == ConnectionState.Closed)
                    await dbContext.Database.OpenConnectionAsync(cancellationToken);

                cmd.Transaction = dbContext.Database.CurrentTransaction?.GetDbTransaction();

                cmd.CommandText = sql.Sql;

                // Add query parameters
                foreach (var bind in sql.NamedBindings)
                {
                    var p = cmd.CreateParameter();
                    p.ParameterName = bind.Key;
                    if (bind.Value != null && bind.Value.GetType().GetNullableUnderlyingType().IsEnum)
                    {
                        p.Value = bind.Value.ToString();
                    }
                    else
                    {
                        p.Value = bind.Value;
                    }
                    cmd.Parameters.Add(p);
                }

                // Add query variables
                foreach (var v in sql.Query.Variables)
                {
                    var p = cmd.CreateParameter();
                    p.ParameterName = v.Key;
                    if (v.Value != null && v.Value.GetType().GetNullableUnderlyingType().IsEnum)
                    {
                        p.Value = v.Value.ToString();
                    }
                    else
                    {
                        p.Value = v.Value;
                    }
                    cmd.Parameters.Add(p);
                }

                var sqlResult = await cmd.ExecuteScalarAsync(cancellationToken);

                if (sqlResult == null || sqlResult == DBNull.Value)
                    result = default;
                else
                    result = (TResult)sqlResult;
            }

            return result;
        }

        public static async Task<TResult> ToSingleOrDefault<TResult>(this DbContext dbContext, Query query, CancellationToken cancellationToken = default) where TResult : new()
        {
            var list = ToEnumerableAsync<TResult>(dbContext, query, cancellationToken).GetAsyncEnumerator(cancellationToken);

            var first_exists = await list.MoveNextAsync();

            TResult result;

            if (first_exists)
                result = list.Current;
            else
                result = default;

            var second_exists = await list.MoveNextAsync();

            await list.DisposeAsync();

            if (second_exists)
                throw new InvalidOperationException("The input sequence contains more than one element.");
            else
                return result;
        }

        public static async Task<TResult> ToSingleOrDefault<TResult>(this DbContext dbContext, TResult model, Query query, CancellationToken cancellationToken = default) where TResult : new()
        {
            return await ToSingleOrDefault<TResult>(dbContext, query, cancellationToken);
        }

        public static async Task<TResult> ToFirstOrDefault<TResult>(this DbContext dbContext, Query query, CancellationToken cancellationToken = default) where TResult : new()
        {
            var list = ToEnumerableAsync<TResult>(dbContext, query, cancellationToken).GetAsyncEnumerator(cancellationToken);

            var exists = await list.MoveNextAsync();

            TResult result;

            if (exists)
                result = list.Current;
            else
                result = default;

            await list.DisposeAsync();

            return result;
        }

        public static async Task<TResult> ToFirstOrDefault<TResult>(this DbContext dbContext, TResult model, Query query, CancellationToken cancellationToken = default) where TResult : new()
        {
            return await ToFirstOrDefault<TResult>(dbContext, query, cancellationToken);
        }

        public static async Task<List<TResult>> ToListAsync<TResult>(this DbContext dbContext, Query query, CancellationToken cancellationToken = default) where TResult : new()
        {
            var items = new List<TResult>();

            await foreach (var item in dbContext.ToEnumerableAsync<TResult>(query, cancellationToken))
                items.Add(item);

            return items;
        }

        public static async Task<List<TResult>> ToListAsync<TResult>(this DbContext dbContext, TResult model, Query query, CancellationToken cancellationToken = default) where TResult : new()
        {
            return await ToListAsync<TResult>(dbContext, query, cancellationToken);
        }

        public static async IAsyncEnumerable<TResult> ToEnumerableAsync<TResult>(this DbContext dbContext, Query query, [EnumeratorCancellation] CancellationToken cancellationToken = default) where TResult : new()
        {
            var compiler = new SqlServerCompiler() { UseLegacyPagination = true };

            // Compile to sql query
            var sql = compiler.Compile(query);

#if DEBUG
            Debug.WriteLine(sql);
#endif

            using (var cmd = dbContext.CreateCommand())
            {
                if (cmd.Connection.State == ConnectionState.Closed)
                    await dbContext.Database.OpenConnectionAsync(cancellationToken);

                cmd.CommandText = sql.Sql;

                // Add query parameters
                foreach (var bind in sql.NamedBindings)
                {
                    var p = cmd.CreateParameter();
                    p.ParameterName = bind.Key;
                    if (bind.Value != null && bind.Value.GetType().GetNullableUnderlyingType().IsEnum)
                    {
                        p.Value = bind.Value.ToString();
                    }
                    else
                    {
                        p.Value = bind.Value;
                    }
                    cmd.Parameters.Add(p);
                }

                // Add query variables
                foreach (var v in sql.Query.Variables)
                {
                    var p = cmd.CreateParameter();
                    p.ParameterName = v.Key;
                    if (v.Value != null && v.Value.GetType().GetNullableUnderlyingType().IsEnum)
                    {
                        p.Value = v.Value.ToString();
                    }
                    else
                    {
                        p.Value = v.Value;
                    }
                    cmd.Parameters.Add(p);
                }

                // Read rows
                await foreach (var item in cmd.ReadObjectsAsync<TResult>(cancellationToken))
                    yield return item;
            }
        }

        public static async IAsyncEnumerable<TResult> ToEnumerableAsync<TResult>(this DbContext dbContext, TResult model, Query query, [EnumeratorCancellation] CancellationToken cancellationToken = default) where TResult : new()
        {
            await foreach (var item in ToEnumerableAsync<TResult>(dbContext, query, cancellationToken))
                yield return item;
        }

        #endregion Execution

        #region Misc

        public static void AutoIncrementOff(this DbContext dbContext, Type entity)
        {
            using (var cmd = dbContext.CreateCommand())
            {
                cmd.CommandText = $"SET IDENTITY_INSERT {dbContext.GetTableName(entity)} ON";

                cmd.ExecuteNonQuery();
            }
        }

        public static void AutoIncrementOn(this DbContext dbContext, Type entity)
        {
            using (var cmd = dbContext.CreateCommand())
            {
                cmd.CommandText = $"SET IDENTITY_INSERT {dbContext.GetTableName(entity)} OFF";

                cmd.ExecuteNonQuery();
            }
        }

        public static void LockTable(this DbContext dbContext, Type entity)
        {
            using (var cmd = dbContext.CreateCommand())
            {
                cmd.CommandText = $"SELECT TOP 0 NULL FROM {dbContext.GetTableName(entity)} WITH (TABLOCKX)";

                cmd.ExecuteNonQuery();
            }
        }

        public static string GetTableName(this DbContext dbContext, Type entity)
        {
            if (entity == null)
                throw new ArgumentNullException(nameof(entity));

            var tableAttr = entity.GetCustomAttribute<TableAttribute>();
            if (tableAttr == null)
                throw new ArgumentException($"The entity type '{entity.Name}' is not a table.", nameof(entity));

            if (!string.IsNullOrWhiteSpace(tableAttr.Schema))
            {
                return $"{tableAttr.Schema}.dbo.{tableAttr.Name}";
            }
            else
            {
                return $"{tableAttr.Name}";
            }
        }

        public static string GetDbName(this DbContext dbContext)
        {
            // We can also use this
            // return new SqlConnectionStringBuilder(dbContext.Database.GetConnectionString()).InitialCatalog;

            return dbContext.Database.GetDbConnection().Database;
        }

        /// <summary>
        /// Creates an empty DbCommand
        /// Please remember to dispose DbCommand and DbDataReader afterwards
        /// </summary>
        public static DbCommand CreateCommand(this DbContext dbContext)
        {
            var connection = dbContext.GetOpenConnection();
            var cmd = connection.CreateCommand();

            cmd.Transaction = dbContext.Database.CurrentTransaction?.GetDbTransaction();

            return cmd;
        }

        /// <summary>
        /// Creates a DbCommand containing the <paramref name="query"/>
        /// Please remember to dispose DbCommand and DbDataReader afterwards
        /// </summary>
        public static async Task<DbCommand> CreateDbCommand(this DbContext dbContext, Query query, CancellationToken cancellationToken = default)
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

#if DEBUG
            Debug.WriteLine(sql);
#endif

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

        #endregion Misc

        #region Private Methods

        private static async IAsyncEnumerable<TResult> ReadObjectsAsync<TResult>(this DbCommand cmd, [EnumeratorCancellation] CancellationToken cancellationToken = default) where TResult : new()
        {
            // Execute query
            using var reader = await cmd.ExecuteReaderAsync(cancellationToken);

            if (!reader.HasRows)
                yield break;

            // Get property list from Class or Tuple (struct)
            var properties = GetPropertiesOfModel(typeof(TResult));

            // Read rows
            while (await reader.ReadAsync(cancellationToken))
            {
                TResult item;

                // Read columns
                if (properties.Count == 0) // Single column struct (eg. int, string or enum)
                {
                    item = (TResult)ParseDbField(reader[0], typeof(TResult));
                }
                else
                {
                    item = (TResult)Activator.CreateInstance(typeof(TResult), true);

                    for (int i = 0; i < reader.FieldCount; ++i)
                    {
                        string columnName = reader.GetName(i);

                        if (properties.TryGetValue(columnName, out var mappedProperty))
                        {
                            var value = reader[columnName];
                            var propertyType = mappedProperty.Member.GetMemberType();

                            try
                            {
                                value = ParseDbField(value, propertyType);

                                if (!object.Equals(value, GetDefaultValue(propertyType)))
                                {
                                    var propertyItem = GetMemberItem(item, mappedProperty, properties);

                                    if (mappedProperty.Member.MemberType == MemberTypes.Property)
                                    {
                                        (mappedProperty.Member as PropertyInfo).SetValue(propertyItem, value);
                                    }
                                    else
                                    {
                                        // https://social.msdn.microsoft.com/Forums/vstudio/en-US/33284e33-d004-4b76-bc0f-50100ec46bf1/fieldinfosetvalue-dont-work-in-struct?forum=csharpgeneral
                                        object obj_ref = propertyItem;
                                        (mappedProperty.Member as FieldInfo).SetValue(obj_ref, value);
                                        propertyItem = (TResult)obj_ref;
                                        if (mappedProperty.Parent == null)
                                            item = (TResult)obj_ref;
                                    }
                                }
                            }
                            catch (Exception exc)
                            {
                                throw new InvalidCastException($"Could not cast database type {value?.GetType().FullName} into the property type {propertyType.FullName} for property name {typeof(TResult).Name}.{mappedProperty.Member.Name}", exc);
                            }
                        }
                    }
                }

                yield return item;
            }
        }

        private static Type GetMemberType(this MemberInfo member)
        {
            return member.MemberType == MemberTypes.Property
                ? (member as PropertyInfo).PropertyType
                : (member as FieldInfo).FieldType;
        }

        /// <summary>
        /// Gets the current instance of the given member of the root item
        /// </summary>
        private static object GetMemberItem(object rootItem, (MemberInfo Member, MemberInfo Parent) member, IDictionary<string, (MemberInfo Member, MemberInfo Parent)> allProperties)
        {
            if (member.Member == null)
                throw new ArgumentNullException();

            // If the parent member doesn't exist, then the current instance is the root item
            if (member.Parent == null)
            {
                return rootItem
                    ?? Activator.CreateInstance(member.Member.GetMemberType(), true);
            }
            else
            {
                // Find parent member
                var parentMember = allProperties.Single(x => x.Value.Member == member.Parent);

                // Currently only one level is available. TODO: Write Unit test and make it work for more than one level
                var item = parentMember.Value.Member is PropertyInfo
                    ? (parentMember.Value.Member as PropertyInfo).GetValue(rootItem)
                    : (parentMember.Value.Member as FieldInfo).GetValue(rootItem);

                // Create an instance of the parent's member if it is null
                if (item == null)
                {
                    if (parentMember.Value.Member.MemberType == MemberTypes.Property)
                    {
                        var parentMemberType = (parentMember.Value.Member as PropertyInfo).PropertyType;
                        item = Activator.CreateInstance(parentMemberType, true);
                        (parentMember.Value.Member as PropertyInfo).SetValue(rootItem, item);
                    }
                    else // Tuple
                    {
                        var parentMemberType = (parentMember.Value.Member as FieldInfo).FieldType;
                        item = Activator.CreateInstance(parentMemberType, true);
                        (parentMember.Value.Member as FieldInfo).SetValue(rootItem, item);
                    }
                }

                // Return the current instance of the parent member
                if (parentMember.Value.Parent == null)
                {
                    return item;
                }
                else
                {
                    // If there are other parents down to the hierarhy, make sure all of them are not null.
                    GetMemberItem(item, parentMember.Value, allProperties);
                    return item;
                }
            }
        }

        /// <summary>
        /// Gets all available properties of the model including child classes and their properties
        /// </summary>
        /// <param name="modelType"></param>
        /// <returns>IDictionary<"Full_Member_Path", (MemberInfo Member, MemberInfo Parent)></returns>
        /// <exception cref="ArgumentNullException"></exception>
        private static IDictionary<string, (MemberInfo Member, MemberInfo Parent)> GetPropertiesOfModel(Type modelType)
        {
            if (modelType == null)
                throw new ArgumentNullException(nameof(modelType));

            IDictionary<string, (MemberInfo Member, MemberInfo Parent)> result = null;

            if (modelType.Name.StartsWith("ValueTuple`"))
            {
                result = modelType.GetFields().ToDictionary(x => x.Name, x => (Member: (MemberInfo)x, Parent: (MemberInfo)null), StringComparer.InvariantCultureIgnoreCase);
            }
            else
            {
                result = new Dictionary<string, (MemberInfo Member, MemberInfo Parent)>(StringComparer.InvariantCultureIgnoreCase);
                foreach (var property in modelType.GetWritableProperties())
                {
                    result.Add(property.Name, (Member: (MemberInfo)property, Parent: (MemberInfo)null));

                    if (property.PropertyType.Name.StartsWith("ValueTuple`") ||
                        (property.PropertyType.IsClass && property.PropertyType != typeof(string) && !typeof(IEnumerable).IsAssignableFrom(property.PropertyType)))
                    {
                        var children = GetPropertiesOfModel(property.PropertyType);
                        foreach (var child in children)
                        {
                            result.Add(property.Name + "_" + child.Key, (Member: child.Value.Member, Parent: (MemberInfo)property));
                        }
                    }
                }
            }

            return result;
        }

        private static object ParseDbField(object value, Type toType)
        {
            if (value is DBNull)
                value = GetDefaultValue(toType);

            if (value != null)
            {
                if (toType.IsEnum || toType.GetNullableUnderlyingType().IsEnum)
                {
                    value = Enum.Parse(toType.IsEnum ? toType : toType.GetNullableUnderlyingType(), value.ToString(), true);
                }
                else
                {
                    value = value.ConvertType(toType);
                }
            }

            return value;
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

        private static DbConnection GetOpenConnection(this DbContext dbContext)
        {
            var connection = dbContext.Database.GetDbConnection();

            if (connection.State != ConnectionState.Open)
                connection.Open();

            return connection;
        }

        #endregion Private Methods
    }
}