using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.Serialization.Formatters;



namespace RedisStack
{
    class Program
    {
        static void Main(string[] args)
        {
            // DefaultRun();
            //  MakeLots();

            // Stopwatch watch = Stopwatch.StartNew();

            List<Person> personsSelection = RedisAdaptor.RetrievePersonObjects(Gender.FEMALE);

            //watch.Stop();
            //Console.WriteLine(watch.Elapsed);

            Console.WriteLine(personsSelection.Count);
            Console.ReadKey();


        }

        private static void MakeLots()
        {

            for (var i = 0; i < 500000; i++)
            {

                Person person = new Person(Util.GetAnyName(), Gender.FEMALE, Country.USA, DateTime.Now.Date.AddMinutes(1));
                RedisAdaptor.StorePersonObject(person);

                if (i % 10000 == 0)
                {
                    Console.WriteLine(i);
                }


            }


        }



        private static void DefaultRun()
        {
            const int YEAR = 1971;

            // We create one Person object for every single day in the given year.
            for (int month = 1; month <= 12; ++month)
            {
                for (int day = 1; day <= 31; ++day)
                {
                    try
                    {
                        // Get any random name:
                        string name = Util.GetAnyName();
                        // And a DoB:
                        DateTime dob = new DateTime(YEAR, month, day);
                        // As for the gender, let's alternate:
                        Gender gender = Gender.FEMALE;
                        if (day % 2 == 0)
                        {
                            gender = Gender.MALE;
                        }

                        // And the country, let's round-robin between all three:
                        Country country = Country.INDIA;
                        if (day % 3 == 1)
                        {
                            country = Country.USA;
                        }
                        else if (day % 3 == 2)
                        {
                            country = Country.GB;
                        }

                        // Create a new Person object:
                        Person person = new Person(name, gender, country, dob);
                        //Console.WriteLine ("Created new Person object: {0}", person);

                        // We call the function that will store a new person in Redis:
                        RedisAdaptor.StorePersonObject(person);
                    }
                    catch (Exception)
                    {
                        // If the control reaches here, it means the date was illegal.
                        // So we just shrug your shoulders and move on to the next date.
                        continue;
                    }
                }
            }

            // At this point, we have 365 Person objects as a sorted set in our Redis database.

            // Next, let's take a date range and retrieve Person objects from within that range.
            DateTime fromDate = DateTime.Parse("5-May-" + YEAR);
            DateTime toDate = DateTime.Parse("7-May-" + YEAR);

            List<Person> persons = RedisAdaptor.RetrievePersonObjects(fromDate, toDate);

            Console.WriteLine("Retrieved values in specified date range:");
            foreach (Person person in persons)
            {
                Console.WriteLine(person);
            }

            // Next, let's select some folks who are female AND from the USA.
            // This calls for a set intersection operation.
            List<Person> personsSelection = RedisAdaptor.RetrieveSelection(Gender.FEMALE, Country.USA);

            Console.WriteLine("Retrieved values in selection:");
            foreach (Person person in personsSelection)
            {
                Console.WriteLine(person);
            }

            Console.ReadKey();
        }
    }
}
