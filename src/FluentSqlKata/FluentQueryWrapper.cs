using SqlKata;
using System.Collections.Generic;

namespace FluentSqlKata
{
    internal class FluentQueryWrapper : Query
    {
        internal readonly IDictionary<string, string> Selects = new Dictionary<string, string>(); // <alias, column>
        internal readonly IDictionary<string, string> SelectsRaw = new Dictionary<string, string>(); // <alias, raw_query>
        internal readonly IDictionary<string, string> SelectAggrs = new Dictionary<string, string>(); // <alias, aggregation>

        internal FluentQueryWrapper() : base() { }

        internal FluentQueryWrapper(string table, string comment = null) : base(table, comment: comment) { }
    }
}
