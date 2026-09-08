using FluentSqlKata.Tests.Entities;
using FluentSqlKata.Tests.Helpers;

namespace FluentSqlKata.Tests
{
    public class JoinTests
    {
        [Fact]
        public void T16_LeftJoin()
        {
            Contact myCont = null;
            Customer myCust = null;

            // Table name from class
            FluentQuery.Query()
                .From(() => myCont)
                .LeftJoin(() => myCust, () => myCust.Id, () => myCont.CustomerId)
                .Select(() => myCont.FirstName)
                .AssertSql("SELECT myCont.FirstName FROM [Contacts] AS [myCont] \nLEFT JOIN [Customer] AS [myCust] ON [myCust].[Id] = [myCont].[contact_customer_id]");

            // Custom table name
            FluentQuery.Query()
                .From(() => myCont)
                .LeftJoin("MyCustomer", () => myCust, () => myCust.Id, () => myCont.CustomerId)
                .Select(() => myCont.FirstName)
                .AssertSql("SELECT myCont.FirstName FROM [Contacts] AS [myCont] \nLEFT JOIN [MyCustomer] AS [myCust] ON [myCust].[Id] = [myCont].[contact_customer_id]");
        }

        [Fact]
        public void T17_RightJoin()
        {
            Contact myCont = null;
            Customer myCust = null;

            // Table name from class
            FluentQuery.Query()
                .From(() => myCont)
                .RightJoin(() => myCust, () => myCust.Id, () => myCont.CustomerId)
                .Select(() => myCont.FirstName)
                .AssertSql("SELECT myCont.FirstName FROM [Contacts] AS [myCont] \nRIGHT JOIN [Customer] AS [myCust] ON [myCust].[Id] = [myCont].[contact_customer_id]");

            // Custom table name
            FluentQuery.Query()
                .From(() => myCont)
                .RightJoin("MyCustomer", () => myCust, () => myCust.Id, () => myCont.CustomerId)
                .Select(() => myCont.FirstName)
                .AssertSql("SELECT myCont.FirstName FROM [Contacts] AS [myCont] \nRIGHT JOIN [MyCustomer] AS [myCust] ON [myCust].[Id] = [myCont].[contact_customer_id]");
        }

        [Fact]
        public void T18_CrossJoin()
        {
            Contact myCont = null;
            Customer myCust = null;

            // By entity type: resolves the entity table (not the alias) since the CrossJoin fix
            FluentQuery.Query()
                .From(() => myCont)
                .CrossJoin(() => myCust)
                .AssertSql("SELECT * FROM [Contacts] AS [myCont] \nCROSS JOIN [Customer] AS [myCust]");

            // By table string
            FluentQuery.Query()
                .From(() => myCont)
                .CrossJoin<Customer>("MyCustomers")
                .AssertSql("SELECT * FROM [Contacts] AS [myCont] \nCROSS JOIN [MyCustomers]");
        }

        [Fact]
        public void T19_JoinBuilderMultipleConditions()
        {
            Contact myCont = null;
            Customer myCust = null;

            FluentQuery.Query()
                .From(() => myCont)
                .Join(() => myCust, (join) => join
                    .On(() => myCust.Id, () => myCont.CustomerId)
                    .OrOn(() => myCust.Name, () => myCont.FirstName))
                .Select(() => myCont.FirstName)
                .AssertSql("SELECT myCont.FirstName FROM [Contacts] AS [myCont] \nINNER JOIN [Customer] AS [myCust] ON ([myCust].[Id] = [myCont].[contact_customer_id] OR [myCust].[Name] = [myCont].[FirstName])");
        }

        [Fact]
        public void T20_JoinTypeParam()
        {
            Contact myCont = null;
            Customer myCust = null;

            FluentQuery.Query()
                .From(() => myCont)
                .Join(() => myCust, (join) => join.On(() => myCust.Id, () => myCont.CustomerId), type: "left join")
                .Select(() => myCont.FirstName)
                .AssertSql("SELECT myCont.FirstName FROM [Contacts] AS [myCont] \nLEFT JOIN [Customer] AS [myCust] ON ([myCust].[Id] = [myCont].[contact_customer_id])");
        }

        [Fact]
        public void T21_JoinWithInBuilder()
        {
            Contact myCont = null;
            Customer myCust = null;
            BirdWithSchema bird = null;

            // Note: JoinWith inside a builder join does NOT add the extra table to the compiled SQL
            // (SqlKata only renders the join's base table, while conditions referencing the extra
            // table are kept) - flagged as an upstream SqlKata behavior
            FluentQuery.Query()
                .From(() => myCont)
                .Join(() => myCust, (join) => join
                    .On(() => myCust.Id, () => myCont.CustomerId)
                    .JoinWith(() => bird)
                    .On(() => bird.Name, () => myCont.FirstName))
                .Select(() => myCont.FirstName)
                .AssertSql("SELECT myCont.FirstName FROM [Contacts] AS [myCont] \nINNER JOIN [Customer] AS [myCust] ON ([myCust].[Id] = [myCont].[contact_customer_id] AND [bird].[Name] = [myCont].[FirstName])");
        }
    }
}
