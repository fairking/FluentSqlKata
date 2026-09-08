using SqlKata;
using System;
using System.Collections.Generic;

namespace FluentSqlKata
{
    internal class FluentQueryWrapper : Query
    {
        internal readonly IDictionary<string, string> Selects = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase); // <alias, column>
        internal readonly IDictionary<string, string> SelectsRaw = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase); // <alias, raw_query>
        internal readonly IDictionary<string, string> SelectAggrs = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase); // <alias, aggregation>

        internal FluentQueryWrapper() : base() { }

        internal FluentQueryWrapper(string table, string comment = null) : base(table, comment: comment) { }

        /// <summary>
        /// Creates clones of the same FluentQueryWrapper type, so that Clone() (e.g. used by SqlKata's
        /// With/Paginate or by callers) keeps the alias dictionaries reachable through GetWrapper().
        /// </summary>
        public override Query NewQuery()
        {
            return new FluentQueryWrapper();
        }

        /// <summary>
        /// Clones the query preserving the alias registrations so that alias-dependent methods
        /// (e.g. OrderByAlias) keep working on the clone.
        /// </summary>
        public override Query Clone()
        {
            var clone = (FluentQueryWrapper)base.Clone();

            foreach (var pair in Selects)
                clone.Selects.Add(pair.Key, pair.Value);

            foreach (var pair in SelectsRaw)
                clone.SelectsRaw.Add(pair.Key, pair.Value);

            foreach (var pair in SelectAggrs)
                clone.SelectAggrs.Add(pair.Key, pair.Value);

            return clone;
        }
    }
}
