using SqlKata;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;

namespace FluentSqlKata
{
    public static class FluentQuery
    {
        #region Query/From

        public static Query Query()
        {
            return new FluentQueryWrapper();
        }

        public static Query Query<A>()
        {
            return new FluentQueryWrapper($"{GetTableName<A>()}");
        }

        public static Query Query<A>(Expression<Func<A>> alias)
        {
            return new FluentQueryWrapper($"{GetTableName<A>()} AS {GetAliasName(alias)}");
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

		#region Selects

		public static Query Select<A>(this Query query, Expression<Func<A>> alias, Query subquery)
		{
			var aliasName = GetAliasName(alias);
			query.Select(subquery, aliasName);
			return query;
		}

		public static Query SelectRawFormat<A>(this Query query, Expression<Func<A>> alias, string queryFormat, params Expression<Func<object>>[] columns)
		{
			var aliasName = GetAliasName(alias);
			return query.SelectRawFormat(aliasName, queryFormat, columns: columns);
		}

		public static Query SelectRawFormat(this Query query, string alias, string queryFormat, params Expression<Func<object>>[] columns)
		{
			var queryRaw = FormatQueryRaw(queryFormat, columns);
			query.GetWrapper().SelectsRaw.Add(alias, queryRaw);
			query.SelectRaw($"{queryRaw} AS {alias}");
			return query;
		}

		public static Query Select<A, T>(this Query query, Expression<Func<A>> alias, Expression<Func<T>> column)
		{
			var aliasName = GetAliasName(alias);
			return query.Select(aliasName, column);
		}

		public static Query Select<T>(this Query query, string alias, Expression<Func<T>> column)
		{
			var columnName = $"{GetAliasNameFromPropery(column)}.{GetPropertyName(column)}";
			query.GetWrapper().Selects.Add(alias, columnName);
			query.Select($"{columnName} AS {alias}");
			return query;
		}

		public static Query SelectAll<T>(this Query query)
        {
            query.From<T>();

            foreach (var col in GetColumns<T>())
            {
                var columnName = $"{col.Value}";
                query.GetWrapper().Selects.Add(col.Key, columnName);
                query.Select($"{columnName} AS {col.Key}");
            }

            return query;
        }

		public static Query SelectAll<T>(this Query query, Expression<Func<T>> alias)
		{
			query.From(alias);

			foreach (var col in GetColumns<T>())
			{
				var columnName = $"{GetAliasName(alias)}.{col.Value}";
				query.GetWrapper().Selects.Add(col.Key, columnName);
				query.Select($"{columnName} AS {col.Key}");
			}

			return query;
		}

		public static Query SelectFunc<A, T>(this Query query, Expression<Func<A>> alias, Expression<Func<T>> column, string func, bool aggregate = false)
        {
            var aliasName = GetAliasName<A>(alias);
            var columnName = $"{func}({GetAliasNameFromPropery(column)}.{GetPropertyName(column)})";
            if (aggregate)
                query.GetWrapper().SelectAggrs.Add(aliasName, columnName);
            else
                query.GetWrapper().Selects.Add(aliasName, columnName);
            query.SelectRaw($"{columnName} AS {aliasName}");
            return query;
        }

        #endregion Selects

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

		public static Q WhereColumns<Q, T1>(this Q query, Expression<Func<T1>> column1, string second, string op = "=") where Q : BaseQuery<Q>
		{
			query.WhereColumns($"{GetAliasNameFromPropery(column1)}.{GetPropertyName(column1)}", op, second);
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

		public static Q OrWhereColumns<Q, T1>(this Q query, Expression<Func<T1>> column1, string second, string op = "=") where Q : BaseQuery<Q>
		{
			query.OrWhereColumns($"{GetAliasNameFromPropery(column1)}.{GetPropertyName(column1)}", op, second);
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

        public static Q WhereIn<Q, T>(this Q query, Expression<Func<T>> column, Query subquery) where Q : BaseQuery<Q>
        {
            query.WhereIn($"{GetAliasNameFromPropery(column)}.{GetPropertyName(column)}", subquery);
            return query;
        }

        public static Q OrWhereIn<Q, T>(this Q query, Expression<Func<T>> column, Query subquery) where Q : BaseQuery<Q>
        {
            query.OrWhereIn($"{GetAliasNameFromPropery(column)}.{GetPropertyName(column)}", subquery);
            return query;
        }

        public static Q WhereNotIn<Q, T>(this Q query, Expression<Func<T>> column, Query subquery) where Q : BaseQuery<Q>
        {
            query.WhereNotIn($"{GetAliasNameFromPropery(column)}.{GetPropertyName(column)}", subquery);
            return query;
        }

        public static Q OrWhereNotIn<Q, T>(this Q query, Expression<Func<T>> column, Query subquery) where Q : BaseQuery<Q>
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

        public static Query OrderByAlias<T>(this Query query, Expression<Func<T>> alias)
        {
            var aliasName = GetAliasName(alias);
            query.OrderByAlias(aliasName);
            return query;
        }

        public static Query OrderByAlias(this Query query, string alias)
        {
            if (query.GetWrapper().Selects.TryGetValue(alias, out var select))
            {
                query.OrderBy($"{select}");
            }
            else if (query.GetWrapper().SelectsRaw.TryGetValue(alias, out var selectRaw))
            {
                query.OrderByRaw(selectRaw);
            }
            else if (query.GetWrapper().SelectAggrs.TryGetValue(alias, out var selectAggr))
            {
                query.OrderByRaw(selectAggr);
            }
            else
            {
                throw new ArgumentException($"The alias name '{alias}' not found or not supported.");
            }

            return query;
        }

        public static Query OrderByColumnDesc<T>(this Query query, Expression<Func<T>> column)
        {
            query.OrderByDesc($"{GetAliasNameFromPropery(column)}.{GetPropertyName(column)}");
            return query;
        }

        public static Query OrderByAliasDesc<T>(this Query query, Expression<Func<T>> alias)
        {
            var aliasName = GetAliasName(alias);
            query.OrderByAliasDesc(aliasName);
            return query;
        }

        public static Query OrderByAliasDesc(this Query query, string alias)
        {
            if (query.GetWrapper().Selects.TryGetValue(alias, out var select))
            {
                query.OrderByDesc($"{select}");
            }
            else if (query.GetWrapper().SelectsRaw.TryGetValue(alias, out var selectRaw))
            {
                query.OrderByRaw(selectRaw + " desc");
            }
            else if (query.GetWrapper().SelectAggrs.TryGetValue(alias, out var selectAggr))
            {
                query.OrderByRaw(selectAggr + " desc");
            }
            else
            {
                throw new ArgumentException($"The alias '{alias}' not found or not supported.");
            }

            return query;
        }

        public static Query OrderByRaw(this Query query, string queryFormat, params Expression<Func<object>>[] columns)
        {
            queryFormat = FormatQueryRaw(queryFormat, columns);
            query.OrderByRaw(queryFormat);
            return query;
        }

        #endregion Orders

        #region Aggregations

        public static Query SelectCount<A, T>(this Query query, Expression<Func<A>> alias, Expression<Func<T>> column)
        {
            query.SelectFunc(alias, column, "COUNT", aggregate: true);
            return query;
        }

        public static Query SelectMin<A, T>(this Query query, Expression<Func<A>> alias, Expression<Func<T>> column)
        {
            query.SelectFunc(alias, column, "MIN", aggregate: true);
            return query;
        }

        public static Query SelectMax<A, T>(this Query query, Expression<Func<A>> alias, Expression<Func<T>> column)
        {
            query.SelectFunc(alias, column, "MIN", aggregate: true);
            return query;
        }

        public static Query SelectAvg<A, T>(this Query query, Expression<Func<A>> alias, Expression<Func<T>> column)
        {
            query.SelectFunc(alias, column, "AVG", aggregate: true);
            return query;
        }

        public static Query SelectSum<A, T>(this Query query, Expression<Func<A>> alias, Expression<Func<T>> column)
        {
            query.SelectFunc(alias, column, "SUM", aggregate: true);
            return query;
        }

        public static Query AsCount<A, T>(this Query query, Expression<Func<A>> alias, Expression<Func<T>> column)
        {
            var aliasName = GetAliasName<A>(alias);
            var columnName = $"{GetAliasNameFromPropery(column)}.{GetPropertyName(column)}";
            query.GetWrapper().SelectAggrs.Add(aliasName, columnName);
            query.AsCount(new[] { $"{columnName} AS {aliasName}" });
            return query;
        }

        public static Query AsAvg<A, T>(this Query query, Expression<Func<A>> alias, Expression<Func<T>> column)
        {
            var aliasName = GetAliasName<A>(alias);
            var columnName = $"{GetAliasNameFromPropery(column)}.{GetPropertyName(column)}";
            query.GetWrapper().SelectAggrs.Add(aliasName, columnName);
            query.AsAvg($"{columnName} AS {aliasName}");
            return query;
        }

        public static Query AsAverage<A, T>(this Query query, Expression<Func<A>> alias, Expression<Func<T>> column)
        {
            var aliasName = GetAliasName<A>(alias);
            var columnName = $"{GetAliasNameFromPropery(column)}.{GetPropertyName(column)}";
            query.GetWrapper().SelectAggrs.Add(aliasName, columnName);
            query.AsAverage($"{columnName} AS {aliasName}");
            return query;
        }

        public static Query AsSum<A, T>(this Query query, Expression<Func<A>> alias, Expression<Func<T>> column)
        {
            var aliasName = GetAliasName<A>(alias);
            var columnName = $"{GetAliasNameFromPropery(column)}.{GetPropertyName(column)}";
            query.GetWrapper().SelectAggrs.Add(aliasName, columnName);
            query.AsSum($"{columnName} AS {aliasName}");
            return query;
        }

        public static Query AsMax<A, T>(this Query query, Expression<Func<A>> alias, Expression<Func<T>> column)
        {
            var aliasName = GetAliasName<A>(alias);
            var columnName = $"{GetAliasNameFromPropery(column)}.{GetPropertyName(column)}";
            query.GetWrapper().SelectAggrs.Add(aliasName, columnName);
            query.AsMax($"{columnName} AS {aliasName}");
            return query;
        }

        public static Query AsMin<A, T>(this Query query, Expression<Func<A>> alias, Expression<Func<T>> column)
        {
            var aliasName = GetAliasName<A>(alias);
            var columnName = $"{GetAliasNameFromPropery(column)}.{GetPropertyName(column)}";
            query.GetWrapper().SelectAggrs.Add(aliasName, columnName);
            query.AsMin($"{columnName} AS {aliasName}");
            return query;
        }

        public static Query GroupBy<T>(this Query query, Expression<Func<T>> column)
        {
            var columnName = $"{GetAliasNameFromPropery(column)}.{GetPropertyName(column)}";
            query.GroupBy(new[] { columnName });
            return query;
        }

        public static Query GroupByRaw<T>(this Query query, string queryFormat, params Expression<Func<object>>[] columns)
        {
            var queryRaw = FormatQueryRaw(queryFormat, columns);
            query.GroupByRaw(queryRaw);
            return query;
        }

        #endregion Aggregations

        #region Hevings

        public static Query Having<T>(this Query query, Expression<Func<T>> column, object value, string op = "=")
        {
            query.Having($"{GetAliasNameFromPropery(column)}.{GetPropertyName(column)}", op, value);
            return query;
        }

        public static Query HavingNot<T>(this Query query, Expression<Func<T>> column, object value, string op = "=")
        {
            query.HavingNot($"{GetAliasNameFromPropery(column)}.{GetPropertyName(column)}", op, value);
            return query;
        }

        public static Query OrHaving<T>(this Query query, Expression<Func<T>> column, object value, string op = "=")
        {
            query.OrHaving($"{GetAliasNameFromPropery(column)}.{GetPropertyName(column)}", op, value);
            return query;
        }

        public static Query OrHavingNot<T>(this Query query, Expression<Func<T>> column, object value, string op = "=")
        {
            query.OrHavingNot($"{GetAliasNameFromPropery(column)}.{GetPropertyName(column)}", op, value);
            return query;
        }

        public static Query HavingColumns<T1, T2>(this Query query, Expression<Func<T1>> firstColumn, Expression<Func<T2>> secondColumn, string op = "=")
        {
            query.HavingColumns($"{GetAliasNameFromPropery(firstColumn)}.{GetPropertyName(firstColumn)}", op, $"{GetAliasNameFromPropery(secondColumn)}.{GetPropertyName(secondColumn)}");
            return query;
        }

        public static Query OrHavingColumns<T1, T2>(this Query query, Expression<Func<T1>> firstColumn, Expression<Func<T2>> secondColumn, string op = "=")
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

        private static FluentQueryWrapper GetWrapper(this Query query)
        {
            return query as FluentQueryWrapper ?? throw new Exception("Cannot execute operation because SqlKata query wasn't instantiated from the FluentQuery. Use 'FluentQuery.Query()' instead of 'new Query()'.");
        }

        private static string GetTableName<A>()
        {
            var attribute = typeof(A).GetCustomAttribute<System.ComponentModel.DataAnnotations.Schema.TableAttribute>();

            var tableName = attribute?.Name;
            var schemaName = attribute?.Schema;

            if (string.IsNullOrWhiteSpace(tableName))
                tableName = typeof(A).Name;

            if (!string.IsNullOrWhiteSpace(schemaName))
                tableName = schemaName + "." + tableName;

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
            var memberName = GetMemberName(property, parent: parent);

            if (memberName != null)
                return memberName.Replace(".", "_");
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
                    if (((LambdaExpression)expression).Body.NodeType == ExpressionType.Convert)
                        return GetMemberName((((LambdaExpression)expression).Body as UnaryExpression).Operand, parent: parent);

					var memberExpression = ((LambdaExpression)expression).Body as MemberExpression
						?? throw new NotSupportedException(expression.NodeType.ToString(), new Exception($"Cannot get member name from expression {expression}."));

                    if (parent)
                        return (memberExpression.Expression as MemberExpression)?.Member.Name
                            ?? throw new NotSupportedException(expression.NodeType.ToString(), new Exception($"Cannot get parent member name from expression {expression}."));
					else
                        return memberExpression.Member.GetCustomAttribute<System.ComponentModel.DataAnnotations.Schema.ColumnAttribute>()?.Name
						    ?? memberExpression.Member.GetCustomAttribute<SqlKata.ColumnAttribute>()?.Name
						    ?? FormatNestedMemberName(memberExpression);

                default:
                    throw new NotSupportedException(expression.NodeType.ToString(),
                        new Exception($"Cannot get member name from expression {expression}."));
            }
        }

        private static string FormatNestedMemberName(MemberExpression expression)
        {
            if (expression == null)
                throw new ArgumentNullException(nameof(expression));

            var name = expression.Member.Name;

            while (expression.Expression is MemberExpression memberExpression && memberExpression.Member.MemberType == MemberTypes.Property)
            {
                name = memberExpression.Member.Name + "." + name;
                expression = memberExpression;
			}

            return name;
		}

        private static string FormatQueryRaw(string queryFormat, params Expression<Func<object>>[] columns)
        {
            if (columns != null && columns.Length > 0)
                queryFormat = string.Format(queryFormat, columns.Select(x => $"{GetAliasNameFromPropery(x)}.{GetPropertyName(x)}").ToArray());

            return queryFormat;
        }

        private static IDictionary<string, string> GetColumns<T>()
        {
            var columns = new Dictionary<string, string>();

            var properties = typeof(T).GetProperties(BindingFlags.Public | BindingFlags.Instance);

            foreach (var prop in properties)
            {
                if (prop.GetCustomAttribute<System.ComponentModel.DataAnnotations.Schema.NotMappedAttribute>() != null || prop.GetCustomAttribute<IgnoreAttribute>() != null)
                    continue;

                var columnName = prop.GetCustomAttribute<System.ComponentModel.DataAnnotations.Schema.ColumnAttribute>()?.Name
                    ?? prop.GetCustomAttribute<SqlKata.ColumnAttribute>()?.Name
                    ?? prop.Name;

                columns.Add(prop.Name, columnName);
            }

            return columns;
        }

        #endregion Private Methods
    }
}
