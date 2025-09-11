# Usage of FluentSqlKata with Entity Framework Core

Example:

```c#
public class UserService
{
	private readonly DbContext _dbContext;

	public UserService(DbContext dbContext)
	{
		_dbContext = dbContext;
	}

	// Example of using Model/Dto result.
	public async Task<List<UserModel>> GetUsers()
	{
		User u = default;
		Company c = default;
		UserModel m = default;

		var query = _dbContext.Query(() => u)
			.Select(() => m.UserId, () => u.Id)
			.Select(() => m.CompanyId, () => c.Id)
			.Select(() => m.UserName, () => u.Name)
			.Select(() => m.CompanyName, () => c.Name)

			.Join(() => c, () => u.CompanyId, () => c.Id)

			.OrderByAlias(() => m.UserName);

		var result = await _dbContext.ToListAsync(m, query);

		return result;
	}

	// Example of using Tuple result.
	public async Task<List<string>> GetUserNames()
	{
		User u = default;
		(int Id, string Name) m = default;

		var query = _dbContext.Query(() => u)
			.Select(() => m.Id, () => u.Id)
			.Select(() => m.Name, () => u.Name)

			.OrderByColumn(() => u.Name);

		var result = await _dbContext.ToListAsync(m, query);

		return result.Select(x => x.Name).ToList();
	}
}
```

