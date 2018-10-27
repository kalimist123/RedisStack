using System;

namespace RedisStack {
    public enum Gender {
        MALE = 1,
        FEMALE = 2,
    }

    public enum Country {
        INDIA = 1,
        USA = 2,
        GB = 3,
    }

    public class Person {
        private string _name;
        private Gender _gender;
        private Country _country;
        private DateTime _dob;

        public Person (string name, Gender gender, Country country, DateTime dob) {
            _name = name;
            _gender = gender;
            _country = country;
            _dob = dob;
        }

        public string Name {
            get {
                return _name;
            }
        }

        public Gender Gender {
            get {
                return _gender;
            }
        }

        public Country Country {
            get {
                return _country;
            }
        }

        public DateTime DoB {
            get {
                return _dob;
            }
        }

        public override string ToString () {
            string countryAsStr = string.Empty;
            switch (_country) {
                case Country.INDIA:
                    countryAsStr = "India";
                    break;
                case Country.USA:
                    countryAsStr = "USA";
                    break;
                case Country.GB:
                    countryAsStr = "Great Britain";
                    break;
                default:
                    countryAsStr = "Unknown";
                    break;
            }

            return string.Format (
                "Name: {0}, Gender:{1} Dob: {2}",
                _name, _gender == Gender.MALE ? "Male" : "Female", countryAsStr, _dob
                );
        }
    }
}
