using System;
using System.Collections.Generic;
using System.Diagnostics;
using Newtonsoft.Json;
using StackExchange.Redis;
using NetJSON;

namespace RedisStack
{
    static class RedisAdaptor
    {
        const string REDIS_HOST = "127.0.0.1";

        private static ConnectionMultiplexer _redis;

        // Date of birth key:
        const string REDIS_DOB_INDEX = "REDIS_DOB_INDEX";

        // Gender keys:
        const string REDIS_MALE_INDEX = "REDIS_MALE_INDEX";
        const string REDIS_FEMALE_INDEX = "REDIS_FEMALE_INDEX";

        // Country keys:
        const string REDIS_C_IN_INDEX = "REDIS_C_IN_INDEX";
        const string REDIS_C_USA_INDEX = "REDIS_C_USA_INDEX";
        const string REDIS_C_GB_INDEX = "REDIS_C_GB_INDEX";

        static RedisAdaptor()
        {
            // First, init the connection:
            _redis = ConnectionMultiplexer.Connect(REDIS_HOST);
        }

        public static void StorePersonObject(Person person)
        {
            // We first JSONize the object so that it's easier to save:
            string personJson = JsonConvert.SerializeObject(person);
            //  Console.WriteLine("JSONized new Person object: {0}", personJson);

            // And save it to Redis.

            // First, get the database object:
            IDatabase db = _redis.GetDatabase();

            // Bear in mind that Redis is fundamentally a key-value store that does not provide indexes out of the box.
            // We therefore work our way around this by creating and managing our own indexes.

            // The first index that we have is for gender.
            // We have two sets for this in Redis: one for males and the other for females.
            if (person.Gender == Gender.MALE)
            {
                db.SetAdd(REDIS_MALE_INDEX, personJson);
            }
            else
            {
                db.SetAdd(REDIS_FEMALE_INDEX, personJson);
            }

            // Next, we index by country.
            if (person.Country == Country.INDIA)
            {
                db.SetAdd(REDIS_C_IN_INDEX, personJson);
            }
            else if (person.Country == Country.USA)
            {
                db.SetAdd(REDIS_C_USA_INDEX, personJson);
            }
            else if (person.Country == Country.GB)
            {
                db.SetAdd(REDIS_C_GB_INDEX, personJson);
            }

            // Next, we need to create an index to be able to retrieve values that are in a particular date range.

            // Since we need to index by date, we use the sorted set structure in Redis. Sorted sets require
            // a score (a real) to save a record. Therefore, in our case, we will use the
            // DoB's `ticks' value as the score.
            double dateTicks = (double)person.DoB.Ticks;

            db.SortedSetAdd(REDIS_DOB_INDEX, personJson, dateTicks);
        }

        public static List<Person> RetrievePersonObjects(DateTime fromDate, DateTime toDate)
        {
            // First. let's convert the dates to tick values:
            double fromTicks = fromDate.Ticks;
            double toTicks = toDate.Ticks;

            // And retrieve values from the sorted set.

            // First, get the database object:
            IDatabase db = _redis.GetDatabase();

            Stopwatch watch = Stopwatch.StartNew();
            RedisValue[] vals = db.SortedSetRangeByScore(REDIS_DOB_INDEX, fromTicks, toTicks);

            watch.Stop();
            Console.WriteLine($"Redis Fetch finished:{watch.Elapsed}");
            watch = Stopwatch.StartNew();
            List<Person> opList = new List<Person>();
            foreach (RedisValue val in vals)
            {
                string personJson = val.ToString();
                //   Person person = JsonConvert.DeserializeObject<Person>(personJson);
                
                var person = NetJSON.NetJSON.Deserialize<Person>(personJson);
                opList.Add(person);
            }
            watch.Stop();
            Console.WriteLine($"List Object Create finished:{watch.Elapsed}");
            return opList;
        }

        public static List<Person> RetrievePersonObjects(Gender gender)
        {
            // First, get the database object:
            IDatabase db = _redis.GetDatabase();

            string keyToUse = gender == Gender.MALE ? REDIS_MALE_INDEX : REDIS_FEMALE_INDEX;

            Stopwatch watch = Stopwatch.StartNew();
            RedisValue[] vals = db.SetMembers(keyToUse);

            watch.Stop();
            Console.WriteLine($"Redis Fetch finished:{watch.Elapsed}");

            List<Person> opList = new List<Person>();
            watch = Stopwatch.StartNew();
            foreach (RedisValue val in vals)
            {
                //string personJson = val.ToString();
                //Person person = JsonConvert.DeserializeObject<Person>(personJson);
                var person = NetJSON.NetJSON.Deserialize<Person>(val.ToString());
                opList.Add(person);
                
            }
            watch.Stop();
            Console.WriteLine($"List Object Create finished:{watch.Elapsed}");
            return opList;
        }

        public static List<Person> RetrievePersonObjects(Country country)
        {
            // First, get the database object:
            IDatabase db = _redis.GetDatabase();

            string keyToUse = REDIS_C_IN_INDEX;
            if (country == Country.USA)
            {
                keyToUse = REDIS_C_USA_INDEX;
            }
            else if (country == Country.GB)
            {
                keyToUse = REDIS_C_GB_INDEX;
            }

            RedisValue[] vals = db.SetMembers(keyToUse);

            List<Person> opList = new List<Person>();
            foreach (RedisValue val in vals)
            {
                //string personJson = val.ToString();
                //Person person = JsonConvert.DeserializeObject<Person>(personJson);
                var person = NetJSON.NetJSON.Deserialize<Person>(val.ToString());
                opList.Add(person);
                opList.Add(person);
            }

            return opList;
        }

        public static List<Person> RetrieveSelection(Gender gender, Country country)
        {
            // First, get the database object:
            IDatabase db = _redis.GetDatabase();

            string keyToUseGender = gender == Gender.MALE ? REDIS_MALE_INDEX : REDIS_FEMALE_INDEX;
            string keyToUseCountry = REDIS_C_IN_INDEX;
            if (country == Country.USA)
            {
                keyToUseCountry = REDIS_C_USA_INDEX;
            }
            else if (country == Country.GB)
            {
                keyToUseCountry = REDIS_C_GB_INDEX;
            }

            RedisKey[] keys = new RedisKey[] { keyToUseGender, keyToUseCountry };

            RedisValue[] vals = db.SetCombine(SetOperation.Intersect, keys);



            List<Person> opList = new List<Person>();
            foreach (RedisValue val in vals)
            {
                string personJson = val.ToString();
                Person person = JsonConvert.DeserializeObject<Person>(personJson);
                opList.Add(person);
            }

            return opList;
        }
    }
}
