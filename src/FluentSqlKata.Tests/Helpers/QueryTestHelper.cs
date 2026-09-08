using SqlKata;
using SqlKata.Compilers;

namespace FluentSqlKata.Tests.Helpers
{
    /// <summary>Small helpers to reduce boilerplate when asserting compiled SQL.</summary>
    public static class QueryTestHelper
    {
        public static string Sql(this Query query, Compiler compiler = null)
        {
            return (compiler ?? new SqlServerCompiler()).Compile(query).ToString();
        }

        public static void AssertSql(this Query query, string expected, Compiler compiler = null)
        {
            var actual = query.Sql(compiler);

            if (actual != expected)
                throw new Xunit.Sdk.XunitException($"Compiled SQL mismatch.\nExpected: {expected}\nActual:   {actual}");
        }
    }
}
