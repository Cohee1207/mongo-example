using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;
using MongoDB.Driver;
using System.Linq;
using System.Collections.Generic;
using System;

public interface IEntity
{
    public ObjectId Id { get; set; }
}

public class MyClass : IEntity
{
    [BsonId]
    [BsonIgnoreIfDefault]
    public ObjectId Id { get; set; }

    public string StringData { get; set; }
}

public class Order : IEntity
{
    public ObjectId Id { get; set; }

    public int CustId {get; set;}

    public int Amount { get; set; }

    public string Status { get; set; }
}

public class OrderGrouping
{
    public int Id { get; set; }

    public int Total { get; set; }
}


public class Repository<TEntity> where TEntity : IEntity
{
    protected readonly IMongoCollection<TEntity> collection;

    public Repository(IMongoDatabase database)
    {
        collection = database.GetCollection<TEntity>(typeof(TEntity).Name);
    }

    public ObjectId Insert(TEntity entity)
    {
        collection.InsertOne(entity);
        return entity.Id;
    }

    public TEntity Get(ObjectId id)
    {
        var filter = Builders<TEntity>.Filter.Eq(e => e.Id, id);
        return collection.Find(filter).FirstOrDefault();
    }
}

public class OrderRepository : Repository<Order>
{
    public OrderRepository(IMongoDatabase database) : base(database)
    {
        collection.Indexes.CreateOne(new CreateIndexModel<Order>("{ Status: 1 }"));
    }

    public List<OrderGrouping> Aggregate()
    {
        collection.Find(o => (o.Amount > 100 && o.Status == "B") || (o.CustId == 1 && o.Status == "A" && o.Amount != 0));

        var builder = Builders<Order>.Filter;
        var filter = (builder.Gt(o => o.Amount, 100) & builder.Eq(o => o.Status, "B")) | (builder.Eq(o => o.CustId, 1) & builder.Eq(o => o.Status, "A") & builder.Ne(o => o.Amount, 0));

        var aggregation = collection
            .Aggregate(new AggregateOptions { AllowDiskUse = true })
            .Match(o => o.Status == "A")
            .Group(o => o.CustId, (group) => new OrderGrouping { Id = group.Key, Total = group.Sum(o => o.Amount) })
            .Sort(Builders<OrderGrouping>.Sort.Ascending(g => g.Total))
            .Limit(10)
            .Skip(0);

        Console.WriteLine(aggregation.ToString());

        return aggregation.ToList();
    }
}

public class Program
{
    public static void Main()
    {
        var client = new MongoClient("mongodb://localhost:27017");
        var database = client.GetDatabase("testdb");
        var genericRepository = new Repository<MyClass>(database);
        var id = genericRepository.Insert(new MyClass { StringData = "Hello, World!" });
        var entity = genericRepository.Get(id);
        Console.WriteLine(entity.StringData);
        var orderRepository = new OrderRepository(database);
        orderRepository.Aggregate();
    }
}
