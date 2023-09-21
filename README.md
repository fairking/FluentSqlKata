# FluentSqlKata

Simple and very concrete, fluent way to build sql queries.

## Features

- Fluent queries (you have more freedom than with `Linq`, `HQL` or `QueryOver`)
- OrderByAlias (Dynamic way to order by columns in runtime)
- Can be easily used along with Entity Framework Core without huge code changes (see [FluentSqlKata.EFCore](https://github.com/fairking/FluentSqlKata/tree/main/src/FluentSqlKata.EFCore/DbContextHelper.cs))
- All standart SqlKata features remain without changes

## Installation

Using dotnet cli
```
$ dotnet add package FluentSqlKata
```
Using Nuget Package Manager
```
PM> Install-Package FluentSqlKata
```

## How to use

``` c#
using FluentSqlKata;

[Fact]
public void Basic()
{
    Customer myCust = null;
    (string CustomerId, string CustomerName) result = default;

    var query = FluentQuery.Query()
        .From(() => myCust)
        .Select(() => result.CustomerId, () => myCust.Id)
        .Select(() => result.CustomerName, () => myCust.Name)
        ;

    var query_str = new SqlServerCompiler().Compile(query).ToString();

    Assert.NotNull(query_str);
    Assert.Equal("SELECT [myCust].[Id] AS [Item1], [myCust].[Name] AS [Item2] FROM [Customer] AS [myCust]", query_str);
}

[Fact]
public void Where()
{
    Customer myCust = null;
    (string CustomerId, string CustomerName) result = default;

    var query = FluentQuery.Query()
        .From(() => myCust)
        .Select(() => result.CustomerId, () => myCust.Id)
        .Select(() => result.CustomerName, () => myCust.Name)
        .Where((q) => q.Where(() => myCust.Name, "John").OrWhereContains(() => myCust.Name, "oh"))
        ;

    var query_str = new SqlServerCompiler().Compile(query).ToString();

    Assert.NotNull(query_str);
    Assert.Equal("SELECT [myCust].[Id] AS [Item1], [myCust].[Name] AS [Item2] FROM [Customer] AS [myCust] WHERE ([myCust].[Name] = 'John' OR LOWER([myCust].[Name]) like '%oh%')", query_str);
}

[Fact]
public void Join()
{
    Contact myCont = null;
    Customer myCust = null;
    (string FirstName, string LastName, string CustomerId, string CustomerName) result = default;

    var query = FluentQuery.Query()
        .From(() => myCont)
        .Join(() => myCust, () => myCust.Id, () => myCont.CustomerId)
        .Select(() => result.FirstName, () => myCont.FirstName)
        .Select(() => result.LastName, () => myCont.LastName)
        .Select(() => result.CustomerId, () => myCont.CustomerId)
        .Select(() => result.CustomerName, () => myCust.Name)
        ;

    var query_str = new SqlServerCompiler().Compile(query).ToString();

    Assert.NotNull(query_str);
    Assert.Equal("SELECT [myCont].[FirstName] AS [Item1], [myCont].[LastName] AS [Item2], [myCont].[contact_customer_id] AS [Item3], [myCust].[Name] AS [Item4] FROM [Contacts] AS [myCont] \nINNER JOIN [Customer] AS [myCust] ON [myCust].[Id] = [myCont].[contact_customer_id]", query_str);
}

[Fact]
public void JoinBuilder()
{
    Contact myCont = null;
    Customer myCust = null;
    (string FirstName, string LastName, string CustomerId) result = default;

    var query = FluentQuery.Query()
        .From(() => myCont)
        .Join(() => myCust, (join) => join.WhereColumns(() => myCust.Id, () => myCont.CustomerId))
        .Select(() => result.FirstName, () => myCont.FirstName)
        .Select(() => result.LastName, () => myCont.LastName)
        .Select(() => result.CustomerId, () => myCust.Id)
        ;

    var query_str = new SqlServerCompiler().Compile(query).ToString();

    Assert.NotNull(query_str);
    Assert.Equal("SELECT [myCont].[FirstName] AS [Item1], [myCont].[LastName] AS [Item2], [myCust].[Id] AS [Item3] FROM [Contacts] AS [myCont] \nINNER JOIN [Customer] AS [myCust] ON ([myCust].[Id] = [myCont].[contact_customer_id])", query_str);
}

[Fact]
public void GroupBy()
{
    Customer myCust = null;
    Contact myCont = null;

    (string CustomerName, int ContactCount) result = default;

    var query = FluentQuery.Query()
        .From(() => myCont)
        .Join(() => myCust, (join) => join.WhereColumns(() => myCust.Id, () => myCont.CustomerId))
        .SelectCount(() => result.ContactCount, () => myCont.Id)
        .Select(() => result.CustomerName, () => myCust.Name)
        .GroupBy(() => myCust.Name)
        ;

    var query_str = new SqlServerCompiler().Compile(query).ToString();

    Assert.NotNull(query_str);
    Assert.Equal("SELECT COUNT(myCont.Id) AS Item2, [myCust].[Name] AS [Item1] FROM [Contacts] AS [myCont] \nINNER JOIN [Customer] AS [myCust] ON ([myCust].[Id] = [myCont].[contact_customer_id]) GROUP BY [myCust].[Name]", query_str);
}

[Fact]
public void OrderBy()
{
    Customer myCust = null;

    var query = FluentQuery.Query()
        .SelectAll(() => myCust)
        .OrderByColumn(() => myCust.Name)
        ;

    var query_str = new SqlServerCompiler().Compile(query).ToString();

    Assert.NotNull(query_str);
    Assert.Equal("SELECT [myCust].[Name] AS [Name], [myCust].[Id] AS [Id] FROM [Customer] AS [myCust] ORDER BY [myCust].[Name]", query_str);
}

[Fact]
public void OrderByAlias()
{
    Customer myCust = null;
    CustomerModel model = null;

    var query = FluentQuery.Query()
        .From(() => myCust)
        .SelectRaw(() => model.Name, "ISNULL({0}, 'Uknown')", () => myCust.Name)
        .OrderByAlias(() => model.Name)
        ;

    var query_str = new SqlServerCompiler().Compile(query).ToString();

    Assert.NotNull(query_str);
    Assert.Equal("SELECT ISNULL(myCust.Name, 'Uknown') AS Name FROM [Customer] AS [myCust] ORDER BY ISNULL(myCust.Name, 'Uknown')", query_str);
}
```

## Nuget

[FluentSqlKata](https://www.nuget.org/packages/FluentSqlKata/)

## How to contribute

If you have any issues please provide us with Unit Test Example.

To become a contributer please create an issue ticket with such enqury.

## Donations

Donate with [Ӿ nano crypto (XNO)](https://nano.org).

[![FluentSqlKata Donations](https://gitlab.com/fairking/sqlkata.queryman/-/raw/master/Resources/Donations_QRCode_nano_1sygjbke.png)](https://nanocrawler.cc/explorer/account/nano_1sygjbkepdcu5diiekf15ar6m6utfgf9rr9tkd6zi8mkq7yza34kiyjpgt9g)

Thank you!