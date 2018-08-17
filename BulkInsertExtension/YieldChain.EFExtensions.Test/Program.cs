using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
//using YLOUDAL;
using YieldChain.EFExtension.BulkExtensions.MySql;

namespace BulkInsertTest
{
    class Program
    {
        static void Main(string[] args)
        {
            //var trades = new List<trade>();

            //for (var i = 0; i < 1000; ++i)
            //{
            //    trades.Add(new trade()
            //    {
            //        UnderlyingCode = "Dummy",
            //        TradeDate = DateTime.Now,
            //        TradePrice = 3.14,
            //        OptId = 1
            //    });
            //}           

            //using (var db = new YLContext())
            //{
            //    Console.WriteLine("Start normal process");
            //    db.trade.AddRange(trades);
            //    db.SaveChanges();
            //    var tradesToDelete = db.trade.Where(x => x.UnderlyingCode == "Dummy").ToList();
            //    db.BulkDelete(tradesToDelete);
            //    db.BulkInsert(trades);
            //    Console.WriteLine("Done!");
            //}

            //using (var db = new YLContext())
            //{
            //    using (var trans = new BulkTransaction(db))
            //    {
            //        var tradesToDelete = db.trade.Where(x => x.UnderlyingCode == "Dummy").ToList();
            //        db.BulkDelete(tradesToDelete);
            //        db.BulkInsert(trades);
            //        trans.Commit();
            //    }
            //}
        }
    }
}
