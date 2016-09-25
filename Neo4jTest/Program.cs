using AntaresVision.Tracking.BusinessLogic.Hierarchy;
using AntaresVision.Tracking.Data;
using Neo4j.Driver.V1;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Neo4jTest
{
    class Program
    {
        private static System.Collections.Concurrent.ConcurrentBag<string> timings = new System.Collections.Concurrent.ConcurrentBag<string>();

        static void Main(string[] args)
        {
            string host = "localhost";
            if (args.Length > 0)
                host = args[0];

            using (var driver = GraphDatabase.Driver($"bolt://{host}", AuthTokens.Basic("neo4j", "antares1")))
            {
                using (var session = driver.Session())
                {
                    
                    switch (args[1])
                    {
                        case "command":
                            { 
                                var sw = Stopwatch.StartNew();
                                var result = session.Run(args[2]);
                                if (result != null)
                                {
                                    result.Consume();
                                }
                                sw.Stop();
                                Console.WriteLine($"Elapsed: { sw.ElapsedMilliseconds}ms");
                            }
                            break;

                        case "hierarchy":
                            hierarchy(session, args[2]);
                            break;
                    }
                }
            }
        }

        private static Dictionary<string, NtinDefinition> ntins = new Dictionary<string, NtinDefinition>();

        private static void hierarchy(ISession session, string dbkey)
        {
            var sw = Stopwatch.StartNew();
            var ntinsResults = session.Run($"MATCH (n:NTIN) RETURN n");
            if (ntinsResults != null)
            {
                foreach (var result in ntinsResults)
                {
                    var ntin = result["n"].As<INode>();
                    ntins.Add(ntin.Properties["DbKey"].As<string>(),
                        new NtinDefinition
                        {
                            Id = ntin.Properties["Id"].As<int>(),
                            Ntin = ntin.Properties["Ntin"].As<string>()
                        });

                }
            }
            sw.Stop();
            Console.WriteLine($"NTINs loaded. Elapsed: { sw.ElapsedMilliseconds}ms");

            var items = new List<Item>();

            sw.Restart();
            //var itemsResults = session.Run($"MATCH (parent:Item {{DbKey:'{dbkey}'}}) with parent, collect({{level: 0, parentDbKey: NULL, item: parent}}) as parentItem MATCH (parent)-[c: CONTAINS *]->(child) with parentItem + collect({{level: size(c), parentDbKey: parent.DbKey, item: child}}) as hierarchy UNWIND hierarchy as item RETURN item;");
            var itemsResults = session.Run($"MATCH p = (:Item{{DbKey:'{dbkey}'}})-[r*0..]->(x) RETURN startNode(last(r)).DbKey as DbKey, x as item;");
            if (itemsResults != null)
            {
                foreach(var result in itemsResults)
                {
                    var item = result["item"].As<INode>();
                    var parentDbKey = result["DbKey"].As<String>();
                    var i = new Item
                    {
                        NtinId = ntins[item.Properties["DbKey"].As<string>().Substring(0, 8)].Id,
                        Serial = item.Properties["DbKey"].As<string>().Substring(8),
                        
                        OtherData = new Dictionary<string, string>()
                        {
                            { "ParentDbKey", parentDbKey },
                            { "DbKey", item.Properties["DbKey"].As<string>() },
                            { "Id", item.Id.ToString() }
                        },
                    };
                    if (parentDbKey != null)
                    {
                        i.ParentNtinId = ntins[parentDbKey.Substring(0, 8)].Id;
                        i.ParentSerial = parentDbKey.Substring(8);
                    }
                    items.Add(i);
                }
            }
            sw.Stop();
            Console.WriteLine($"Pallet children list populated from NEODB. Elapsed: { sw.ElapsedMilliseconds}ms");

            sw.Restart();
            var hierarchy = buildHierarchy(items);
            sw.Stop();
            Console.WriteLine($"Hierarchy built in memory. Descendants:{hierarchy.CountDescendants()}. Elapsed: { sw.ElapsedMilliseconds}ms");
        }

        private static HierarchyItem buildHierarchy(IEnumerable<Item> elements)
        {
            RootHierarchyItem root = new RootHierarchyItem();
            Dictionary<string, HierarchyItem> map = new Dictionary<string, HierarchyItem>();
            foreach (Item itm in elements)
            {
                HierarchyItem e = new HierarchyItem(itm);
                try
                {
                    map.Add(itm.OtherData["DbKey"], e);
                }
                catch (Exception ex)
                {
                    Console.WriteLine(ex.ToString());
                }
            }
            foreach (HierarchyItem i in map.Values)
            {
                if (i.AssociatedItem.OtherData["ParentDbKey"] != null && map.ContainsKey(i.AssociatedItem.OtherData["ParentDbKey"]))
                {
                    map[i.AssociatedItem.OtherData["ParentDbKey"]].AddChild(i);
                }
                else
                {
                    if (i.HasParent())
                    {
                    }
                    root.AddChild(i);
                }
            }
            return root;
        }
    }
}
