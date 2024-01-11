// See https://aka.ms/new-console-template for more information

using Bogus;
using ClickHouse.Client.Copy;
using ClickHouse.Client.Utility;

Console.WriteLine("Hello, World!");
const string connectionString =
    "Compression=True;Timeout=10000;Host=localhost;Port=8123;Database=test;Username=yowko;Password=pass.123";
const string tableName = "test.orders";

await using var connection = new ClickHouse.Client.ADO.ClickHouseConnection(connectionString);
await InsertSingle(new Order
{
    Id = 1882682734613504000,
    OrderDate = new DateTime(2023, 01, 01, 00, 00, 00, DateTimeKind.Utc),
    ProductId = 100,
    OrderType = 1,
    Amount = 100.001M
});
//await InsertSingle(GetOrders(1).FirstOrDefault());
//await InsertBulk(GetOrders(10));
//await Query();

//await Update();

//await Delete();

//await DeleteLightweight();

//await DeleteTruncate();

async Task InsertSingle(Order order)
{
    var command = connection.CreateCommand();

    command.AddParameter("id", order.Id);
    command.AddParameter("order_date", order.OrderDate);
    command.AddParameter("product_id", order.ProductId);
    command.AddParameter("order_type", order.OrderType);
    command.AddParameter("amount", order.Amount);
    command.CommandText =
        $"INSERT INTO {tableName} VALUES ({{id:UInt64}},{{order_date:DateTime}},{{product_id:UInt32}},{{order_type:UInt8}},{{amount:Decimal(12,6)}})";
    try
    {
        await command.ExecuteNonQueryAsync();
    }
    catch (Exception e)
    {
        Console.WriteLine(e);
    }
    finally
    {
        connection.Close();
    }
}

async Task InsertBulk(Order[] orders)
{
    using var bulkCopy = new ClickHouseBulkCopy(connection)
    {
        DestinationTableName = tableName,
        ColumnNames = new[] { "id", "order_date", "product_id", "order_type", "amount" },
        BatchSize = orders.Length
    };
    await bulkCopy.InitAsync();
    var ordersObj = orders.Select(order => new object[]
        { order.Id, order.OrderDate, order.ProductId, order.OrderType, order.Amount });

    try
    {
        await bulkCopy.WriteToServerAsync(ordersObj);
    }
    catch (Exception e)
    {
        Console.WriteLine(e);
    }
    finally
    {
        connection?.Close();
    }
}

async Task Query()
{
    try
    {
        await using var reader = await connection.ExecuteReaderAsync($"select * from {tableName};");
        while (reader.Read())
            Console.WriteLine(
                $"{reader["id"]} - {reader["order_date"]} - {reader["product_id"]} - {reader["order_type"]} - {reader["amount"]}");
    }
    catch (Exception e)
    {
        Console.WriteLine(e);
    }
    finally
    {
        connection?.Close();
    }
}

async Task Update()
{
    try
    {
        await connection.ExecuteScalarAsync(
            $"ALTER TABLE {tableName} UPDATE amount = 100.002 WHERE id= 1882682734613504000");
    }
    catch (Exception e)
    {
        Console.WriteLine(e);
    }
    finally
    {
        connection?.Close();
    }
}

async Task Delete()
{
    try
    {
        await connection.ExecuteScalarAsync($"ALTER TABLE {tableName} DELETE WHERE id= 1882682734613504000");
    }
    catch (Exception e)
    {
        Console.WriteLine(e);
    }
    finally
    {
        connection?.Close();
    }
}

async Task DeleteTruncate()
{
    try
    {
        await connection.ExecuteScalarAsync($"TRUNCATE TABLE {tableName}");
    }
    catch (Exception e)
    {
        Console.WriteLine(e);
    }
    finally
    {
        connection?.Close();
    }
}

async Task DeleteLightweight()
{
    try
    {
        await connection.ExecuteScalarAsync($"DELETE FROM {tableName} WHERE id= 1882682734613504000");
    }
    catch (Exception e)
    {
        Console.WriteLine(e);
    }
    finally
    {
        connection?.Close();
    }
}


Order[] GetOrders(int BatchSize)
{
    var startDate = new DateTime(2021, 01, 01, 00, 00, 00, DateTimeKind.Utc);
    var order = new Faker<Order>()
        .RuleFor(a => a.Id, f => f.Random.ULong())
        .RuleFor(a => a.OrderDate, f => startDate.AddDays(f.Random.Number(0, 365 * 3)))
        .RuleFor(a => a.ProductId, f => f.Random.Number(1, 10000))
        .RuleFor(a => a.OrderType, f => f.Random.SByte(1, 10))
        .RuleFor(a => a.Amount, f => f.Random.Decimal(0M, 100000M));
    return order.Generate(BatchSize).ToArray();
}

public class Order
{
    public ulong Id { get; set; }
    public DateTime OrderDate { get; set; }
    public int ProductId { get; set; }
    public sbyte OrderType { get; set; }
    public decimal Amount { get; set; }
}