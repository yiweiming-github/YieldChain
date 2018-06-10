# BulkInsertExtension
在使用EntityFramework的项目中支持BulkInsert和BulkDelete。

### 谁需要这个库？
- 对EntityFramework中大批量插入、删除操作的效率不满意，但又不愿意使用付费插件(比如[entityframework-extensions](http://entityframework-extensions.net/))。可以尝试一下这个BulkInsertExtension。

### 如何使用？
```
    using YieldChain.EFExtension.BulkExtensions.MySql;
    
    static void Main(string[] args)
    {
        //use your own table classes
        var trades = new List<trade>();

        for (var i = 0; i < 1000; ++i)
        {
            trades.Add(new trade()
            {
                UnderlyingCode = "Dummy",
                TradeDate = DateTime.Now,
                TradePrice = 3.14,
                OptId = 1
            });
        }           

        using (var db = new YLContext()) // use your own DbContext
        {
            // normal EF way
            Console.WriteLine("Start normal process");
            db.trade.AddRange(trades);
            db.SaveChanges();
            
            // use BulkInsertExtension. 
            // db.SaveChanges() is not required.
            var tradesToDelete = db.trade.Where(x => x.UnderlyingCode == "Dummy").ToList();
            db.BulkDelete(tradesToDelete);
            db.BulkInsert(trades);
            
            Console.WriteLine("Done!");
        }
    }

```

### 为什么只支持MySql？
- 因为我们在用MySql，而SqlServer和Oracle等都有相对成熟便利的解决方法，只有MySql在这方面的支持稍欠缺。
