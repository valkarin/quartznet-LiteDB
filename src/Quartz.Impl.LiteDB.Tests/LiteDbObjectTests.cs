using System;
using FluentAssertions;
using LiteDB;
using Quartz.Impl.Calendar;
using Xunit;

namespace Quartz.Impl.LiteDB.Tests
{
    public class Calendars
    {
        public ObjectId Id { get; set; }
        public AnnualCalendar AnnualCalendar { get; set; }
        public CronCalendar CronCalendar { get; set; }
        public DailyCalendar DailyCalendar { get; set; }
        public HolidayCalendar HolidayCalendar { get; set; }
        public MonthlyCalendar MonthlyCalendar { get; set; }
        public WeeklyCalendar WeeklyCalendar { get; set; }
    }

    public class LiteDbObjectTests : IClassFixture<TestCleanUp>
    {
        private readonly string _filename = "";

        public LiteDbObjectTests()
        {
            _filename = TestConsts.GenerateFilename;
            // Custom types to be created.
            BsonMapper.Global.RegisterType(
                serialize: value => new BsonValue(value.Id),
                deserialize: bsonValue => TimeZoneInfo.FindSystemTimeZoneById(bsonValue.AsString));
            BsonMapper.Global.RegisterType(
                serialize: value =>
                {
                    var doc = new BsonDocument
                    {
                        ["Description"] = new BsonValue(value.Description),
                        ["Expression"] = new BsonValue(value.CronExpression.CronExpressionString),
                        ["TimeZoneId"] = value.TimeZone.Id
                    };
                    return doc;
                },
                deserialize: bsonValue =>
                {
                    var cron = new CronCalendar(bsonValue["Expression"].AsString)
                    {
                        TimeZone = TimeZoneInfo.FindSystemTimeZoneById(bsonValue["TimeZoneId"].AsString),
                        Description = bsonValue["Description"].AsString
                    };
                    return cron;
                });
        }

        [Fact]
        public void TestCalendars()
        {
            using var db = new LiteDatabase(new ConnectionString($"Filename={_filename};")
                { Connection = ConnectionType.Shared });
            var col = db.GetCollection<Calendars>();

            var baseCalendar = new BaseCalendar
            {
                TimeZone = TimeZoneInfo.Utc,
                Description = "Test BaseCalendar"
            };

            var annualCalendar = new AnnualCalendar
            {
                TimeZone = TimeZoneInfo.Utc,
                Description = "Test AnnualCalendar",
                CalendarBase = baseCalendar
            };
            annualCalendar.SetDayExcluded(DateTime.Today, true);

            var cronCalendar = new CronCalendar("0 0 * * * ?")
            {
                TimeZone = TimeZoneInfo.Local,
                Description = "Test CronCalendar",
                CalendarBase = null
            };

            var dailyCalendar =
                new DailyCalendar(new DateTime(2000, 1, 1, 10, 0, 0), new DateTime(2000, 1, 1, 12, 30, 0))
                {
                    TimeZone = TimeZoneInfo.Utc,
                    Description = null,
                    CalendarBase = baseCalendar,
                    InvertTimeRange = true
                };

            var holidayCalendar = new HolidayCalendar
            {
                TimeZone = TimeZoneInfo.Utc,
                Description = "Test HolidayCalendar",
                CalendarBase = baseCalendar
            };
            holidayCalendar.AddExcludedDate(DateTime.Today);

            var monthlyCalendar = new MonthlyCalendar
            {
                TimeZone = TimeZoneInfo.Utc,
                Description = "Test MonthlyCalendar",
                CalendarBase = baseCalendar
            };
            monthlyCalendar.SetDayExcluded(10, true);
            monthlyCalendar.SetDayExcluded(20, true);
            monthlyCalendar.SetDayExcluded(30, true);

            var weeklyCalendar = new WeeklyCalendar
            {
                TimeZone = TimeZoneInfo.Utc,
                Description = "Test WeeklyCalendar",
                CalendarBase = baseCalendar
            };
            weeklyCalendar.SetDayExcluded(DayOfWeek.Wednesday, true);
            weeklyCalendar.SetDayExcluded(DayOfWeek.Thursday, true);
            weeklyCalendar.SetDayExcluded(DayOfWeek.Friday, true);

            var calendars = new Calendars
            {
                AnnualCalendar = annualCalendar,
                CronCalendar = cronCalendar,
                DailyCalendar = dailyCalendar,
                HolidayCalendar = holidayCalendar,
                MonthlyCalendar = monthlyCalendar,
                WeeklyCalendar = weeklyCalendar
            };

            var id = col.Insert(calendars);
            db.Commit();

            var fromDatabase = col.FindOne(cal => cal.Id == id.AsObjectId);

            calendars.Should().BeEquivalentTo(fromDatabase);
        }
    }
}