using System.Collections.Generic;
using System.IO;
using System.Text;
using NUnit.Framework;

namespace Ns.Test {
    public class Tests {
        [SetUp]
        public void Setup() {
        }

        [Test]
        public void TestBasicSequence() {
            const char c = 'x';
            var arr = new[] {0, 1, 2, 3, 4, 5};
            var list = new List<int>();
            list.AddRange(arr);
            const string str = "VS.ネビュラグレイ";
            var b = new StringBuilder();
            while (b.Length < 4096 * 4)
                b.Append(str);
            var strLong = b.ToString();
            Assert.IsTrue(strLong.Length >= 4096 * 4);
            const decimal num = 1.05m;
            var dict = new Dictionary<string, string> {
                {"halo", "reach"},
                {"tron", "evolution"},
                {"portal", "2"},
                {"xbox", "360"}
            };
            var strArr = new[] {
                "sounds",
                "like",
                "there's",
                "a",
                "lot",
                "of",
                "hoopla"
            };
            var ms = new MemoryStream();
            var ns = new NetSerializer(ms);
            ns.Serialize(c);
            ns.Serialize(arr);
            ns.Serialize(list);
            ns.Serialize(str);
            ns.Serialize(num);
            ns.Serialize(dict);
            ns.Serialize(strArr);
            ns.Serialize(strLong);
            ms.Position = 0;
            var c2 = ns.Deserialize<char>();
            var arr2 = ns.Deserialize<int[]>();
            var list2 = ns.Deserialize<List<int>>();
            var str2 = ns.Deserialize<string>();
            var num2 = ns.Deserialize<decimal>();
            var dict2 = ns.Deserialize<Dictionary<string, string>>();
            var strArr2 = ns.Deserialize<string[]>();
            var strLong2 = ns.Deserialize<string>();
            Assert.AreEqual(c, c2);
            Assert.AreEqual(arr, arr2);
            CollectionAssert.AreEquivalent(list, list2);
            Assert.AreEqual(str, str2);
            Assert.AreEqual(num, num2);
            CollectionAssert.AreEquivalent(dict, dict2);
            CollectionAssert.AreEquivalent(strArr, strArr2);
            Assert.AreEqual(strLong, strLong2);
        }
    }
}