using System;
using System.Text;

namespace RedisStack {
    static class Util {
        public static string GetAnyName () {
            string guidAsStr = Guid.NewGuid ().ToString ();

            StringBuilder opStr = new StringBuilder ();
            opStr.Append ('p');

            foreach (char ch in guidAsStr) {
                if (!Char.IsLetterOrDigit (ch)) {
                    continue;
                }
                opStr.Append (ch);
            }

            return opStr.ToString ();
        }
    }
}
