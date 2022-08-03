using SqlKata;
using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;

namespace FluentSqlKata
{
    public static class FluentSqlKata
    {
        #region Query/From

        public static Query Query()
        {
            return new Query();
        }

        public static Query Query<A>()
        {
            return new Query($"{GetTableName<A>()}");
        }

        public static Query Query<A>(Expression<Func<A>> alias)
        {
            return new Query($"{GetTableName<A>()} AS {GetAliasName(alias)}");
        }

        public static Query From<A>(this Query query)
        {
            return query.From($"{GetTableName<A>()}");
        }

        public static Join From<A>(this Join query)
        {
            return query.From($"{GetTableName<A>()}");
        }

        public static Q From<Q, A>(this Q query, Expression<Func<A>> alias) where Q : BaseQuery<Q>
        {
            return query.From($"{GetTableName<A>()} AS {GetAliasName(alias)}");
        }

        #endregion Query/From

        #region Where

        public static Q WhereRaw<Q>(this Q query, string queryFormat, params Expression<Func<object>>[] columns) where Q : BaseQuery<Q>
        {
            queryFormat = FormatQueryRaw(queryFormat, columns);
            query.WhereRaw(queryFormat);
            return query;
        }

        public static Q OrWhereRaw<Q>(this Q query, string queryFormat, params Expression<Func<object>>[] columns) where Q : BaseQuery<Q>
        {
            queryFormat = FormatQueryRaw(queryFormat, columns);
            query.OrWhereRaw(queryFormat);
            return query;
        }

        public static Q Where<Q, T>(this Q query, Expression<Func<T>> column, object value, string op = "=") where Q : BaseQuery<Q>
        {
            query.Where($"{GetAliasNameFromPropery(column)}.{GetPropertyName(column)}", op, value);
            return query;
        }

        public static Q OrWhere<Q, T>(this Q query, Expression<Func<T>> column, object value, string op = "=") where Q : BaseQuery<Q>
        {
            query.OrWhere($"{GetAliasNameFromPropery(column)}.{GetPropertyName(column)}", op, value);
            return query;
        }

        public static Q WhereNot<Q, T>(this Q query, Expression<Func<T>> column, object value, string op = "=") where Q : BaseQuery<Q>
        {
            query.WhereNot($"{GetAliasNameFromPropery(column)}.{GetPropertyName(column)}", op, value);
            return query;
        }

        public static Q OrWhereNot<Q, T>(this Q query, Expression<Func<T>> column, object value, string op = "=") where Q : BaseQuery<Q>
        {
            query.OrWhereNot($"{GetAliasNameFromPropery(column)}.{GetPropertyName(column)}", op, value);
            return query;
        }

        public static Q WhereColumns<Q, T1, T2>(this Q query, Expression<Func<T1>> column1, Expression<Func<T2>> column2, string op = "=") where Q : BaseQuery<Q>
        {
            query.WhereColumns(
                $"{GetAliasNameFromPropery(column1)}.{GetPropertyName(column1)}",
                op,
                $"{GetAliasNameFromPropery(column2)}.{GetPropertyName(column2)}");
            return query;
        }

        public static Q OrWhereColumns<Q, T1, T2>(this Q query, Expression<Func<T1>> column1, Expression<Func<T2>> column2, string op = "=") where Q : BaseQuery<Q>
        {
            query.OrWhereColumns(
                $"{GetAliasNameFromPropery(column1)}.{GetPropertyName(column1)}",
                op,
                $"{GetAliasNameFromPropery(column2)}.{GetPropertyName(column2)}");
            return query;
        }

        public static Q WhereNull<Q, T>(this Q query, Expression<Func<T>> column) where Q : BaseQuery<Q>
        {
            query.WhereNull($"{GetAliasNameFromPropery(column)}.{GetPropertyName(column)}");
            return query;
        }

        public static Q OrWhereNull<Q, T>(this Q query, Expression<Func<T>> column) where Q : BaseQuery<Q>
        {
            query.OrWhereNull($"{GetAliasNameFromPropery(column)}.{GetPropertyName(column)}");
            return query;
        }

        public static Q WhereNotNull<Q, T>(this Q query, Expression<Func<T>> column) where Q : BaseQuery<Q>
        {
            query.WhereNotNull($"{GetAliasNameFromPropery(column)}.{GetPropertyName(column)}");
            return query;
        }

        public static Q OrWhereNotNull<Q, T>(this Q query, Expression<Func<T>> column) where Q : BaseQuery<Q>
        {
            query.OrWhereNotNull($"{GetAliasNameFromPropery(column)}.{GetPropertyName(column)}");
            return query;
        }

        public static Q WhereTrue<Q, T>(this Q query, Expression<Func<T>> column) where Q : BaseQuery<Q>
        {
            query.WhereTrue($"{GetAliasNameFromPropery(column)}.{GetPropertyName(column)}");
            return query;
        }

        public static Q OrWhereTrue<Q, T>(this Q query, Expression<Func<T>> column) where Q : BaseQuery<Q>
        {
            query.OrWhereTrue($"{GetAliasNameFromPropery(column)}.{GetPropertyName(column)}");
            return query;
        }

        public static Q WhereFalse<Q, T>(this Q query, Expression<Func<T>> column) where Q : BaseQuery<Q>
        {
            query.WhereFalse($"{GetAliasNameFromPropery(column)}.{GetPropertyName(column)}");
            return query;
        }

        public static Q OrWhereFalse<Q, T>(this Q query, Expression<Func<T>> column) where Q : BaseQuery<Q>
        {
            query.OrWhereFalse($"{GetAliasNameFromPropery(column)}.{GetPropertyName(column)}");
            return query;
        }

        public static Q WhereLike<Q, T>(this Q query, Expression<Func<T>> column, object value, bool caseSensitive = false) where Q : BaseQuery<Q>
        {
            query.WhereLike($"{GetAliasNameFromPropery(column)}.{GetPropertyName(column)}", value, caseSensitive: caseSensitive);
            return query;
        }

        public static Q OrWhereLike<Q, T>(this Q query, Expression<Func<T>> column, object value, bool caseSensitive = false) where Q : BaseQuery<Q>
        {
            query.OrWhereLike($"{GetAliasNameFromPropery(column)}.{GetPropertyName(column)}", value, caseSensitive: caseSensitive);
            return query;
        }

        public static Q WhereNotLike<Q, T>(this Q query, Expression<Func<T>> column, object value, bool caseSensitive = false) where Q : BaseQuery<Q>
        {
            query.WhereNotLike($"{GetAliasNameFromPropery(column)}.{GetPropertyName(column)}", value, caseSensitive: caseSensitive);
            return query;
        }

        public static Q OrWhereNotLike<Q, T>(this Q query, Expression<Func<T>> column, object value, bool caseSensitive = false) where Q : BaseQuery<Q>
        {
            query.OrWhereNotLike($"{GetAliasNameFromPropery(column)}.{GetPropertyName(column)}", value, caseSensitive: caseSensitive);
            return query;
        }

        public static Q WhereStarts<Q, T>(this Q query, Expression<Func<T>> column, object value, bool caseSensitive = false) where Q : BaseQuery<Q>
        {
            query.WhereStarts($"{GetAliasNameFromPropery(column)}.{GetPropertyName(column)}", value, caseSensitive: caseSensitive);
            return query;
        }

        public static Q OrWhereStarts<Q, T>(this Q query, Expression<Func<T>> column, object value, bool caseSensitive = false) where Q : BaseQuery<Q>
        {
            query.OrWhereStarts($"{GetAliasNameFromPropery(column)}.{GetPropertyName(column)}", value, caseSensitive: caseSensitive);
            return query;
        }

        public static Q WhereNotStarts<Q, T>(this Q query, Expression<Func<T>> column, object value, bool caseSensitive = false) where Q : BaseQuery<Q>
        {
            query.WhereNotStarts($"{GetAliasNameFromPropery(column)}.{GetPropertyName(column)}", value, caseSensitive: caseSensitive);
            return query;
        }

        public static Q OrWhereNotStarts<Q, T>(this Q query, Expression<Func<T>> column, object value, bool caseSensitive = false) where Q : BaseQuery<Q>
        {
            query.OrWhereNotStarts($"{GetAliasNameFromPropery(column)}.{GetPropertyName(column)}", value, caseSensitive: caseSensitive);
            return query;
        }

        public static Q WhereEnds<Q, T>(this Q query, Expression<Func<T>> column, object value, bool caseSensitive = false) where Q : BaseQuery<Q>
        {
            query.WhereEnds($"{GetAliasNameFromPropery(column)}.{GetPropertyName(column)}", value, caseSensitive: caseSensitive);
            return query;
        }

        public static Q OrWhereEnds<Q, T>(this Q query, Expression<Func<T>> column, object value, bool caseSensitive = false) where Q : BaseQuery<Q>
        {
            query.OrWhereEnds($"{GetAliasNameFromPropery(column)}.{GetPropertyName(column)}", value, caseSensitive: caseSensitive);
            return query;
        }

        public static Q WhereNotEnds<Q, T>(this Q query, Expression<Func<T>> column, object value, bool caseSensitive = false) where Q : BaseQuery<Q>
        {
            query.WhereNotEnds($"{GetAliasNameFromPropery(column)}.{GetPropertyName(column)}", value, caseSensitive: caseSensitive);
            return query;
        }

        public static Q OrWhereNotEnds<Q, T>(this Q query, Expression<Func<T>> column, object value, bool caseSensitive = false) where Q : BaseQuery<Q>
        {
            query.OrWhereNotEnds($"{GetAliasNameFromPropery(column)}.{GetPropertyName(column)}", value, caseSensitive: caseSensitive);
            return query;
        }

        public static Q WhereContains<Q, T>(this Q query, Expression<Func<T>> column, object value, bool caseSensitive = false) where Q : BaseQuery<Q>
        {
            query.WhereContains($"{GetAliasNameFromPropery(column)}.{GetPropertyName(column)}", value, caseSensitive: caseSensitive);
            return query;
        }

        public static Q OrWhereContains<Q, T>(this Q query, Expression<Func<T>> column, object value, bool caseSensitive = false) where Q : BaseQuery<Q>
        {
            query.OrWhereContains($"{GetAliasNameFromPropery(column)}.{GetPropertyName(column)}", value, caseSensitive: caseSensitive);
            return query;
        }

        public static Q WhereNotContains<Q, T>(this Q query, Expression<Func<T>> column, object value, bool caseSensitive = false) where Q : BaseQuery<Q>
        {
            query.WhereNotContains($"{GetAliasNameFromPropery(column)}.{GetPropertyName(column)}", value, caseSensitive: caseSensitive);
            return query;
        }

        public static Q OrWhereNotContains<Q, T>(this Q query, Expression<Func<T>> column, object value, bool caseSensitive = false) where Q : BaseQuery<Q>
        {
            query.OrWhereNotContains($"{GetAliasNameFromPropery(column)}.{GetPropertyName(column)}", value, caseSensitive: caseSensitive);
            return query;
        }

        public static Q WhereBetween<Q, T, TValue>(this Q query, Expression<Func<T>> column, TValue lower, TValue higher) where Q : BaseQuery<Q>
        {
            query.WhereBetween($"{GetAliasNameFromPropery(column)}.{GetPropertyName(column)}", lower, higher);
            return query;
        }

        public static Q OrWhereBetween<Q, T, TValue>(this Q query, Expression<Func<T>> column, TValue lower, TValue higher) where Q : BaseQuery<Q>
        {
            query.OrWhereBetween($"{GetAliasNameFromPropery(column)}.{GetPropertyName(column)}", lower, higher);
            return query;
        }

        public static Q WhereNotBetween<Q, T, TValue>(this Q query, Expression<Func<T>> column, TValue lower, TValue higher) where Q : BaseQuery<Q>
        {
            query.WhereNotBetween($"{GetAliasNameFromPropery(column)}.{GetPropertyName(column)}", lower, higher);
            return query;
        }

        public static Q OrWhereNotBetween<Q, T, TValue>(this Q query, Expression<Func<T>> column, TValue lower, TValue higher) where Q : BaseQuery<Q>
        {
            query.OrWhereNotBetween($"{GetAliasNameFromPropery(column)}.{GetPropertyName(column)}", lower, higher);
            return query;
        }

        public static Q WhereIn<Q, T, TValue>(this Q query, Expression<Func<T>> column, IEnumerable<TValue> values) where Q : BaseQuery<Q>
        {
            query.WhereIn($"{GetAliasNameFromPropery(column)}.{GetPropertyName(column)}", values);
            return query;
        }

        public static Q OrWhereIn<Q, T, TValue>(this Q query, Expression<Func<T>> column, IEnumerable<TValue> values) where Q : BaseQuery<Q>
        {
            query.OrWhereIn($"{GetAliasNameFromPropery(column)}.{GetPropertyName(column)}", values);
            return query;
        }

        public static Q WhereNotIn<Q, T, TValue>(this Q query, Expression<Func<T>> column, IEnumerable<TValue> values) where Q : BaseQuery<Q>
        {
            query.WhereNotIn($"{GetAliasNameFromPropery(column)}.{GetPropertyName(column)}", values);
            return query;
        }

        public static Q OrWhereNotIn<Q, T, TValue>(this Q query, Expression<Func<T>> column, IEnumerable<TValue> values) where Q : BaseQuery<Q>
        {
            query.OrWhereNotIn($"{GetAliasNameFromPropery(column)}.{GetPropertyName(column)}", values);
            return query;
        }

        public static Q WhereIn<Q, T, TValue>(this Q query, Expression<Func<T>> column, Query subquery) where Q : BaseQuery<Q>
        {
            query.WhereIn($"{GetAliasNameFromPropery(column)}.{GetPropertyName(column)}", subquery);
            return query;
        }

        public static Q OrWhereIn<Q, T, TValue>(this Q query, Expression<Func<T>> column, Query subquery) where Q : BaseQuery<Q>
        {
            query.OrWhereIn($"{GetAliasNameFromPropery(column)}.{GetPropertyName(column)}", subquery);
            return query;
        }

        public static Q WhereNotIn<Q, T, TValue>(this Q query, Expression<Func<T>> column, Query subquery) where Q : BaseQuery<Q>
        {
            query.WhereNotIn($"{GetAliasNameFromPropery(column)}.{GetPropertyName(column)}", subquery);
            return query;
        }

        public static Q OrWhereNotIn<Q, T, TValue>(this Q query, Expression<Func<T>> column, Query subquery) where Q : BaseQuery<Q>
        {
            query.OrWhereNotIn($"{GetAliasNameFromPropery(column)}.{GetPropertyName(column)}", subquery);
            return query;
        }

        public static Q WhereDatePart<Q, T>(this Q query, string part, Expression<Func<T>> column, object value, string op = "=") where Q : BaseQuery<Q>
        {
            query.WhereDatePart(part, $"{GetAliasNameFromPropery(column)}.{GetPropertyName(column)}", op, value);
            return query;
        }

        public static Q OrWhereDatePart<Q, T>(this Q query, string part, Expression<Func<T>> column, object value, string op = "=") where Q : BaseQuery<Q>
        {
            query.OrWhereDatePart(part, $"{GetAliasNameFromPropery(column)}.{GetPropertyName(column)}", op, value);
            return query;
        }

        public static Q WhereNotDatePart<Q, T>(this Q query, string part, Expression<Func<T>> column, object value, string op = "=") where Q : BaseQuery<Q>
        {
            query.WhereNotDatePart(part, $"{GetAliasNameFromPropery(column)}.{GetPropertyName(column)}", op, value);
            return query;
        }

        public static Q OrWhereNotDatePart<Q, T>(this Q query, string part, Expression<Func<T>> column, object value, string op = "=") where Q : BaseQuery<Q>
        {
            query.OrWhereNotDatePart(part, $"{GetAliasNameFromPropery(column)}.{GetPropertyName(column)}", op, value);
            return query;
        }

        public static Q WhereDate<Q, T>(this Q query, Expression<Func<T>> column, object value, string op = "=") where Q : BaseQuery<Q>
        {
            query.WhereDate($"{GetAliasNameFromPropery(column)}.{GetPropertyName(column)}", op, value);
            return query;
        }

        public static Q OrWhereDate<Q, T>(this Q query, Expression<Func<T>> column, object value, string op = "=") where Q : BaseQuery<Q>
        {
            query.OrWhereDate($"{GetAliasNameFromPropery(column)}.{GetPropertyName(column)}", op, value);
            return query;
        }

        public static Q WhereNotDate<Q, T>(this Q query, Expression<Func<T>> column, object value, string op = "=") where Q : BaseQuery<Q>
        {
            query.WhereNotDate($"{GetAliasNameFromPropery(column)}.{GetPropertyName(column)}", op, value);
            return query;
        }

        public static Q OrWhereNotDate<Q, T>(this Q query, Expression<Func<T>> column, object value, string op = "=") where Q : BaseQuery<Q>
        {
            query.OrWhereNotDate($"{GetAliasNameFromPropery(column)}.{GetPropertyName(column)}", op, value);
            return query;
        }

        public static Q WherTime<Q, T>(this Q query, Expression<Func<T>> column, object value, string op = "=") where Q : BaseQuery<Q>
        {
            query.WhereTime($"{GetAliasNameFromPropery(column)}.{GetPropertyName(column)}", op, value);
            return query;
        }

        public static Q OrWhereTime<Q, T>(this Q query, Expression<Func<T>> column, object value, string op = "=") where Q : BaseQuery<Q>
        {
            query.OrWhereTime($"{GetAliasNameFromPropery(column)}.{GetPropertyName(column)}", op, value);
            return query;
        }

        public static Q WhereNotTime<Q, T>(this Q query, Expression<Func<T>> column, object value, string op = "=") where Q : BaseQuery<Q>
        {
            query.WhereNotTime($"{GetAliasNameFromPropery(column)}.{GetPropertyName(column)}", op, value);
            return query;
        }

        public static Q OrWhereNotTime<Q, T>(this Q query, Expression<Func<T>> column, object value, string op = "=") where Q : BaseQuery<Q>
        {
            query.OrWhereNotTime($"{GetAliasNameFromPropery(column)}.{GetPropertyName(column)}", op, value);
            return query;
        }

        #endregion Where

        #region Selects

        public static Query Select<A>(this Query query, Expression<Func<A>> alias, Query subquery)
        {
            query.Select(subquery, GetAliasName(alias));
            return query;
        }

        public static Query SelectRaw<A>(this Query query, Expression<Func<A>> alias, string queryFormat, params Expression<Func<object>>[] columns)
        {
            queryFormat = FormatQueryRaw(queryFormat, columns);
            query.SelectRaw($"{queryFormat} AS {GetAliasName(alias)}");
            return query;
        }

        public static Query Select<A, T>(this Query query, Expression<Func<A>> alias, Expression<Func<T>> column)
        {
            query.Select($"{GetAliasNameFromPropery(column)}.{GetPropertyName(column)} AS {GetAliasName(alias)}");
            return query;
        }

        #endregion Selects

        #region Joins

        public static Query Join<A, J1, J2>(this Query query, Expression<Func<A>> alias, Expression<Func<J1>> column1, Expression<Func<J2>> column2, string op = "=")
        {
            query.Join(
                $"{GetTableName<A>()} AS {GetAliasName(alias)}",
                $"{GetAliasNameFromPropery(column1)}.{GetPropertyName(column1)}",
                $"{GetAliasNameFromPropery(column2)}.{GetPropertyName(column2)}",
                op: op
            );
            return query;
        }

        public static Query Join<A>(this Query query, Expression<Func<A>> alias, Func<Join, Join> joinQuery, string type = "inner join")
        {
            query.Join(
                $"{GetTableName<A>()} AS {GetAliasName(alias)}",
                joinQuery,
                type: type
            );
            return query;
        }

        public static Query LeftJoin<A, J1, J2>(this Query query, Expression<Func<A>> alias, Expression<Func<J1>> firstColumn, Expression<Func<J2>> secondColumn, string op = "=")
        {
            query.LeftJoin(
                $"{GetTableName<A>()} AS {GetAliasName(alias)}",
                $"{GetAliasNameFromPropery(firstColumn)}.{GetPropertyName(firstColumn)}",
                $"{GetAliasNameFromPropery(secondColumn)}.{GetPropertyName(secondColumn)}",
                op: op
            );
            return query;
        }

        public static Query LeftJoin<A>(this Query query, Expression<Func<A>> alias, Func<Join, Join> joinQuery)
        {
            query.LeftJoin(
                $"{GetTableName<A>()} AS {GetAliasName(alias)}",
                joinQuery
            );
            return query;
        }

        public static Query RightJoin<A, J1, J2>(this Query query, Expression<Func<A>> alias, Expression<Func<J1>> firstColumn, Expression<Func<J2>> secondColumn, string op = "=")
        {
            query.RightJoin(
                $"{GetTableName<A>()} AS {GetAliasName(alias)}",
                $"{GetAliasNameFromPropery(firstColumn)}.{GetPropertyName(firstColumn)}",
                $"{GetAliasNameFromPropery(secondColumn)}.{GetPropertyName(secondColumn)}",
                op: op
            );
            return query;
        }

        public static Query RightJoin<A>(this Query query, Expression<Func<A>> alias, Func<Join, Join> joinQuery)
        {
            query.RightJoin(
                $"{GetTableName<A>()} AS {GetAliasName(alias)}",
                joinQuery
            );
            return query;
        }

        public static Query CrossJoin<A>(this Query query, Expression<Func<A>> alias)
        {
            query.CrossJoin(GetTableName<A>());
            return query;
        }

        #endregion Joins

        #region Orders

        public static Query OrderByColumn<T>(this Query query, Expression<Func<T>> column)
        {
            query.OrderBy($"{GetAliasNameFromPropery(column)}.{GetPropertyName(column)}");
            return query;
        }

        public static Query OrderByColumnDesc<T>(this Query query, Expression<Func<T>> column)
        {
            query.OrderByDesc($"{GetAliasNameFromPropery(column)}.{GetPropertyName(column)}");
            return query;
        }

        public static Query OrderByRaw(this Query query, string queryFormat, params Expression<Func<object>>[] columns)
        {
            queryFormat = FormatQueryRaw(queryFormat, columns);
            query.OrderByRaw(queryFormat);
            return query;
        }

        public static Query OrderByAlias<A>(Expression<Func<A>> alias)
        {
            // TODO: We need to record all selects in order to have an ability to find a right select by alias (see QueryMan)

            throw new NotImplementedException();
        }

        #endregion Orders

        #region Aggregations

        public static Query AsCount(this Query query, params Expression<Func<object>>[] columns)
        {
            var columnNames = columns?.Select(x => $"{GetAliasNameFromPropery(x)}.{GetPropertyName(x)}").ToArray();
            query.AsCount(columnNames);
            return query;
        }

        public static Query AsAvg(this Query query, Expression<Func<object>> column)
        {
            query.AsAvg($"{GetAliasNameFromPropery(column)}.{GetPropertyName(column)}");
            return query;
        }

        public static Query AsAverage(this Query query, Expression<Func<object>> column)
        {
            query.AsAverage($"{GetAliasNameFromPropery(column)}.{GetPropertyName(column)}");
            return query;
        }

        public static Query AsSum(this Query query, Expression<Func<object>> column)
        {
            query.AsSum($"{GetAliasNameFromPropery(column)}.{GetPropertyName(column)}");
            return query;
        }

        public static Query AsMax(this Query query, Expression<Func<object>> column)
        {
            query.AsMax($"{GetAliasNameFromPropery(column)}.{GetPropertyName(column)}");
            return query;
        }

        public static Query AsMin(this Query query, Expression<Func<object>> column)
        {
            query.AsMin($"{GetAliasNameFromPropery(column)}.{GetPropertyName(column)}");
            return query;
        }

        public static Query GroupBy(this Query query)
        {
            // TODO: We need to record all selects in order to have an ability to group by not agregated selects (see QueryMan)

            throw new NotImplementedException();
        }

        #endregion Aggregations

        #region Hevings

        public static Query Having(this Query query, Expression<Func<object>> column, object value, string op = "=")
        {
            query.Having($"{GetAliasNameFromPropery(column)}.{GetPropertyName(column)}", op, value);
            return query;
        }

        public static Query HavingNot(this Query query, Expression<Func<object>> column, object value, string op = "=")
        {
            query.HavingNot($"{GetAliasNameFromPropery(column)}.{GetPropertyName(column)}", op, value);
            return query;
        }

        public static Query OrHaving(this Query query, Expression<Func<object>> column, object value, string op = "=")
        {
            query.OrHaving($"{GetAliasNameFromPropery(column)}.{GetPropertyName(column)}", op, value);
            return query;
        }

        public static Query OrHavingNot(this Query query, Expression<Func<object>> column, object value, string op = "=")
        {
            query.OrHavingNot($"{GetAliasNameFromPropery(column)}.{GetPropertyName(column)}", op, value);
            return query;
        }

        public static Query HavingColumns(this Query query, Expression<Func<object>> firstColumn, Expression<Func<object>> secondColumn, string op = "=")
        {
            query.HavingColumns($"{GetAliasNameFromPropery(firstColumn)}.{GetPropertyName(firstColumn)}", op, $"{GetAliasNameFromPropery(secondColumn)}.{GetPropertyName(secondColumn)}");
            return query;
        }

        public static Query OrHavingColumns(this Query query, Expression<Func<object>> firstColumn, Expression<Func<object>> secondColumn, string op = "=")
        {
            query.OrHavingColumns($"{GetAliasNameFromPropery(firstColumn)}.{GetPropertyName(firstColumn)}", op, $"{GetAliasNameFromPropery(secondColumn)}.{GetPropertyName(secondColumn)}");
            return query;
        }

        // TODO: There are more overloads we heed to implement

        #endregion Havings

        #region Public Methods

        public static string GetColumnName<T>(Expression<Func<T>> property)
        {
            return GetPropertyName<T>(property);
        }

        #endregion Public Methods

        #region Private Methods

        private static string GetTableName<A>()
        {
            var tableName = typeof(A).GetCustomAttribute<TableAttribute>()?.Name;

            if (string.IsNullOrWhiteSpace(tableName))
                tableName = typeof(A).Name;

            return tableName;
        }

        private static string GetAliasName<A>(Expression<Func<A>> alias)
        {
            return GetPropertyName(alias);
        }

        private static string GetAliasNameFromPropery<T>(Expression<Func<T>> property)
        {
            return GetPropertyName(property, parent: true);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="property"></param>
        /// <param name="snake"></param>
        /// <param name="parent">Get parent member name (eg. customer.Id will return "customer" instead of "ID")</param>
        /// <returns></returns>
        /// <exception cref="ArgumentException"></exception>
        private static string GetPropertyName<T>(Expression<Func<T>> property, bool parent = false)
        {
            var propertyName = GetMemberName(property, parent: parent);

            if (propertyName != null)
                return propertyName;
            else
                throw new ArgumentException($"The expression cannot be evaluated");
        }

        private static string GetMemberName(Expression expression, bool parent = false)
        {
            if (expression == null)
                throw new ArgumentNullException(nameof(expression));

            switch (expression.NodeType)
            {
                case ExpressionType.MemberAccess:
                    return parent
                        ? ((MemberExpression)((MemberExpression)expression).Expression).Member.Name
                        : ((MemberExpression)expression).Member.Name;
                case ExpressionType.Convert:
                    return GetMemberName(((UnaryExpression)expression).Operand, parent: parent);
                case ExpressionType.Lambda:
                    return (parent
                            ? ((((LambdaExpression)expression).Body as MemberExpression)?.Expression as MemberExpression)?.Member.Name
                            : (((LambdaExpression)expression).Body as MemberExpression)?.Member.Name
                        ) ?? throw new NotSupportedException(expression.NodeType.ToString(), new Exception($"Cannot get member name from expression {expression}.")); ;
                default:
                    throw new NotSupportedException(expression.NodeType.ToString(),
                        new Exception($"Cannot get member name from expression {expression}."));
            }
        }

        private static string FormatQueryRaw(string queryFormat, params Expression<Func<object>>[] columns)
        {
            if (columns != null && columns.Length > 0)
                queryFormat = string.Format(queryFormat, columns.Select(x => $"{GetAliasNameFromPropery(x)}.{GetPropertyName(x)}").ToArray());

            return queryFormat;
        }

        #endregion Private Methods
    }
}
