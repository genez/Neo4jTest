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
            var host = args[0];

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

                        case "item":
                            item(session, args[2]);
                            break;

                        case "hierarchy":
                            hierarchy(session, args[2]);
                            break;

                        case "all":
                            all(session);
                            break;

                        case "lot_hierarchy":
                            lot_hierarchy(session, args[2]);
                            break;
                    }
                }
            }
        }

        private static Dictionary<string, NtinDefinition> ntins = new Dictionary<string, NtinDefinition>();
        private static void loadNtins(ISession session)
        {
            ntins.Clear();

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
        }

        private static Item fromNode(INode node)
        {
            var i = new Item();
            i.NtinId = ntins[node.Properties["DbKey"].As<string>().Substring(0, 8)].Id;
            i.Serial = node.Properties["DbKey"].As<string>().Substring(8);
            i.Sequence = node.Properties["Sequence"].As<int>();
            i.Type = node.Properties["Type"].As<short>();

            i.OtherData = new Dictionary<string, string>()
            {
                { "DbKey", node.Properties["DbKey"].As<string>() },
                { "Id", node.Id.ToString() }
            };
            return i;
        }

        private static void item(ISession session, string dbkey)
        {
            loadNtins(session);

            var sw = Stopwatch.StartNew();
            var itemsResults = session.Run("MATCH (i:Item {DbKey:{dbk}}) RETURN i LIMIT 1;", new { dbk = dbkey });
            Item i = null;
            if (itemsResults != null)
            {
                foreach (var result in itemsResults)
                {
                    var node = result["i"].As<INode>();
                    i = fromNode(node);
                }
            }
            sw.Stop();
            Console.WriteLine($"Item retrieved from NEODB. Elapsed: { sw.ElapsedMilliseconds}ms");
            Console.WriteLine(i.AsDBXml());
        }

        private static void hierarchy(ISession session, string dbkey)
        {
            loadNtins(session);

            var items = new List<Item>();

            var sw = Stopwatch.StartNew();
            var itemsResults = session.Run("MATCH (:Item {DbKey:{dbk}})-[rel:CONTAINS*0..]->(child) RETURN startNode(last(rel)).DbKey as DbKey, child as item;", new { dbk = dbkey });
            if (itemsResults != null)
            {
                foreach(var result in itemsResults)
                {
                    var node = result["item"].As<INode>();
                    var parentDbKey = result["DbKey"].As<String>();
                    var i = fromNode(node);
                    i.OtherData["ParentDbKey"] = parentDbKey;
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

        private static void all(ISession session)
        {
            loadNtins(session);

            var items = new List<Item>();

            var sw = Stopwatch.StartNew();
            var itemsResults = session.Run($"MATCH b WHERE NOT (a:Item)-[:CONTAINS]->(b:Item);");
            if (itemsResults != null)
            {
                foreach (var result in itemsResults)
                {
                    var node = result["item"].As<INode>();
                    var parentDbKey = result["DbKey"].As<String>();
                    var i = fromNode(node);
                    i.OtherData["ParentDbKey"] = parentDbKey;
                    if (parentDbKey != null)
                    {
                        i.ParentNtinId = ntins[parentDbKey.Substring(0, 8)].Id;
                        i.ParentSerial = parentDbKey.Substring(8);
                    }
                    items.Add(i);
                }
            }
            sw.Stop();
            Console.WriteLine($"Lot children list populated from NEODB. Elapsed: { sw.ElapsedMilliseconds}ms");

            sw.Restart();
            var hierarchy = buildHierarchy(items);
            sw.Stop();
            Console.WriteLine($"Hierarchy built in memory. Descendants:{hierarchy.CountDescendants()}. Elapsed: { sw.ElapsedMilliseconds}ms");
        }

        private static void lot_hierarchy(ISession session, string lot)
        {
            loadNtins(session);

            var items = new List<Item>();

            var sw = Stopwatch.StartNew();
            var itemsResults = session.Run("MATCH (p:Item)-[c:CONTAINS]->(i:Item)-[r:BELONGS_TO]->(l:Lot {Lot:{l}}) RETURN i as item, p.DbKey as DbKey;", new { l = lot });
            if (itemsResults != null)
            {
                foreach (var result in itemsResults)
                {
                    var node = result["item"].As<INode>();
                    var parentDbKey = result["DbKey"].As<String>();
                    var i = fromNode(node);
                    i.OtherData["ParentDbKey"] = parentDbKey;
                    if (parentDbKey != null)
                    {
                        i.ParentNtinId = ntins[parentDbKey.Substring(0, 8)].Id;
                        i.ParentSerial = parentDbKey.Substring(8);
                    }
                    items.Add(i);
                }
            }
            sw.Stop();
            Console.WriteLine($"Lot children list populated from NEODB. Elapsed: { sw.ElapsedMilliseconds}ms");

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
